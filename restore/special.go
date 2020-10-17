// Copyright 2020 RetailNext, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package restore

import (
	"context"
	"os"
	"sort"

	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"

	"github.com/retailnext/cassandrabackup/nodeidentity"
	"github.com/retailnext/cassandrabackup/restore/plan"
	"github.com/retailnext/cassandrabackup/restore/special"
	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
)

func RestoreSpecial(ctx context.Context) error {
	lgr := zap.S()

	options := plan.Options{
		StartAfter:        unixtime.Seconds(*specialCmdNotBefore),
		NotAfter:          unixtime.Seconds(*specialCmdNotAfter),
		Maximize:          false,
		IgnoreIncomplete:  *specialCmdIgnoreIncomplete,
		IgnoreIncremental: *specialCmdIgnoreIncremental,
		IgnoreSnapshots:   *specialCmdIgnoreSnapshots,
	}

	npf := plan.Filter{
		IncludeIndexes: !*specialCmdClusterMode,
	}
	npf.Build(*specialCmdKeyspace, nil)

	var localCBG special.ComponentsByGenerationByLocation
	var nodeCBGs []special.ComponentsByGenerationByLocation

	wpf := make(workerPlanFactory)

	if *specialCmdClusterMode {
		options.Maximize = true
		identities := nodeIdentitiesForCluster(ctx, specialCmdCluster, specialCmdHostnamePattern)
		lgr.Infow("selected_hosts", "identities", identities)

		for _, identity := range identities {
			hostLgr := lgr.With("identity", identity)
			nodePlan, err := plan.Create(ctx, identity, options)
			if err != nil {
				return err
			}
			nodePlan.Filter(npf)
			if len(nodePlan.SelectedManifests) == 0 {
				hostLgr.Warnw("no_backups_found")
				continue
			}

			var nodeCBG special.ComponentsByGenerationByLocation
			var unrecognized []string
			nodeCBG, unrecognized = special.CollateNodePlan(nodePlan)

			if len(unrecognized) > 0 {
				hostLgr.Warnw("unrecognized_files_from_host", "files", unrecognized)
			}
			nodeCBGs = append(nodeCBGs, nodeCBG)

			wpf.add(identity, nodeCBG)
		}

	} else {
		identity := nodeidentity.ForRestore(ctx, specialCmdCluster, specialCmdHostname, specialCmdHostnamePattern)
		lgr = lgr.With("identity", identity)

		nodePlan, err := plan.Create(ctx, identity, options)
		if err != nil {
			return err
		}
		nodePlan.Filter(npf)

		if len(nodePlan.SelectedManifests) == 0 {
			return NoBackupsFound
		}

		lgr.Infow("selected_manifests", "base", nodePlan.SelectedManifests[0], "additional", nodePlan.SelectedManifests[1:])

		if len(nodePlan.ChangedFiles) > 0 {
			for name, history := range nodePlan.ChangedFiles {
				for _, entry := range history {
					lgr.Infow("file_changed", "name", name, "digest", entry.Digest, "manifest", entry.Manifest)
				}
			}
			if !*specialCmdAllowChangedFiles {
				return ChangesDetected
			}
		}

		toUpload, err := special.FindLiveFiles(ctx)
		if err != nil {
			return err
		}

		var unrecognizedInLocal, unrecognizedInPlan []string
		localCBG, unrecognizedInLocal = special.CollateExistingFiles(toUpload)
		var nodeCBG special.ComponentsByGenerationByLocation
		nodeCBG, unrecognizedInPlan = special.CollateNodePlan(nodePlan)
		nodeCBGs = append(nodeCBGs, nodeCBG)

		wpf.add(identity, nodeCBG)
		wpf.add(identity, localCBG)

		for _, name := range unrecognizedInPlan {
			lgr.Warnw("unrecognized_file_from_plan", "name", name)
		}

		for _, name := range unrecognizedInLocal {
			lgr.Warnw("unrecognized_file_from_disk", "name", name)
		}

	}

	// if nodePlan.SelectedManifests[0].ManifestType != manifests.ManifestTypeSnapshot {
	//	return NoSnapshotsFound
	// }

	combinedPlan := special.BuildLocationPlans(localCBG, nodeCBGs...)

	_ = lgr.Sync()

	if describeErr := combinedPlan.DescribeTo(os.Stdout, true, true, true); describeErr != nil {
		return describeErr
	}

	wp := wpf.makeWorkerPlan(combinedPlan.ToRestoreFiles(true))
	workerOptions := WorkerOptions{
		TargetDirectory:       *specialCmdTargetDirectory,
		StagingDirectory:      "/var/lib/cassandra/backuprestore/staging",
		GraveyardDirectory:    "/var/lib/cassandra/backuprestore/graveyard",
		EnsureOwnership:       true,
		ConcurrentDownload:    4,
		ConcurrentVerify:      1,
		NoDownloadToStaging:   !*specialCmdDownloadToStaging,
		NoLinkToTarget:        !*specialCmdLinkToTarget,
		RemoveInvalidAtTarget: false,
	}
	if !workerOptions.NoLinkToTarget {
		workerOptions.NoDownloadToStaging = false
	}

	return Restore(ctx, wp, workerOptions)
}

type workerPlanFactory map[digest.ForRestore]manifests.NodeIdentities

func (wpf workerPlanFactory) add(node manifests.NodeIdentity, cbgl special.ComponentsByGenerationByLocation) {
	for _, cbg := range cbgl {
		for _, comps := range cbg {
			for _, d := range comps {
				nis := wpf[d]
				nis = append(nis, node)
				sort.Sort(nis)
				filteredNis := nis[:0]
				for i, ni := range nis {
					if i == 0 || ni != nis[i-1] {
						filteredNis = append(filteredNis, ni)
					}
				}
				wpf[d] = nis
			}
		}
	}
}

func (wpf workerPlanFactory) makeWorkerPlan(in map[string]digest.ForRestore) WorkerPlan {
	result := make(WorkerPlan, len(in))
	for name, forRestore := range in {
		result[name] = DownloadableFile{
			Digest: forRestore,
			Nodes:  wpf[forRestore],
		}
	}
	return result
}

func toWorkerPlan(in map[string]digest.ForRestore, nodes ...manifests.NodeIdentity) WorkerPlan {
	result := make(WorkerPlan, len(in))
	for name, forRestore := range in {
		result[name] = DownloadableFile{
			Digest: forRestore,
			Nodes:  nodes,
		}
	}
	return result
}
