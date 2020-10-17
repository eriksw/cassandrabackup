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

	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"

	"github.com/retailnext/cassandrabackup/nodeidentity"
	"github.com/retailnext/cassandrabackup/restore/plan"
	"github.com/retailnext/cassandrabackup/restore/special"
	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
)

func RestoreSpecial(ctx context.Context) error {
	identity := nodeidentity.ForRestore(ctx, specialCmdCluster, specialCmdHostname, specialCmdHostnamePattern)
	lgr := zap.S().With("identity", identity)

	options := plan.Options{
		StartAfter:        unixtime.Seconds(*specialCmdNotBefore),
		NotAfter:          unixtime.Seconds(*specialCmdNotAfter),
		Maximize:          false,
		IgnoreIncomplete:  *specialCmdIgnoreIncomplete,
		IgnoreIncremental: *specialCmdIgnoreIncremental,
		IgnoreSnapshots:   *specialCmdIgnoreSnapshots,
	}

	if *specialCmdLinkToTarget {
		// TODO THIS IS A DUMB SAFETY MEASURE BECAUSE I AM TIRED
		// YOU NEED TO REMOVE THIS TO DO THIS FOR SSTABLELOADER
		options.IgnoreIncomplete = true
		options.IgnoreIncremental = true
	}

	nodePlan, err := plan.Create(ctx, identity, options)
	if err != nil {
		return err
	}

	if len(nodePlan.SelectedManifests) == 0 {
		return NoBackupsFound
	}

	npf := plan.Filter{
		IncludeIndexes: true,
	}
	npf.Build(*specialCmdKeyspace, nil)
	nodePlan.Filter(npf)

	// if nodePlan.SelectedManifests[0].ManifestType != manifests.ManifestTypeSnapshot {
	//	return NoSnapshotsFound
	// }

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

	cbgExisting, unrecognizedLocal := special.CollateExistingFiles(toUpload)
	cbgPlan, unrecognizedInPlan := special.CollateNodePlan(nodePlan)

	combinedPlan := special.BuildLocationPlans(cbgExisting, cbgPlan)

	/*
		for location, tables := range p2.FromNodePlan {
			generations := make(map[int][]string)
			for g, tc := range tables {
				var components []string
				for vc := range tc {
					components = append(components, vc.String())
				}
				sort.Strings(components)
				generations[g] = components
			}

			lgr.Infow("from_node_plan", "location", location, "generations", generations)
		}

		for location, tables := range p2.FromLiveFiles {
			generations := make(map[int][]string)
			for g, tc := range tables {
				var components []string
				for vc := range tc {
					components = append(components, vc.String())
				}
				sort.Strings(components)
				generations[g] = components
			}

			lgr.Infow("from_live_files", "location", location, "generations", generations)
		}
	*/

	/*
		for location, locationPlan := range combinedPlan {
			for generation, generationInfo := range locationPlan.Generations {

				var existing, download []string
				for k := range generationInfo.Download {
					download = append(download, k.String())
				}
				for k := range generationInfo.Existing {
					existing = append(existing, k.String())
				}
				sort.Strings(existing)
				sort.Strings(download)

				lgr.Infow("generation_info", "location", location, "generation", generation, "existing", existing, "download", download)
			}
		}
	*/

	for _, name := range unrecognizedInPlan {
		lgr.Warnw("unrecognized_file_from_plan", "name", name)
	}

	for _, name := range unrecognizedLocal {
		lgr.Warnw("unrecognized_file_from_disk", "name", name)
	}

	_ = lgr.Sync()

	if describeErr := combinedPlan.DescribeTo(os.Stdout, true, true, true); describeErr != nil {
		return describeErr
	}

	wp := toWorkerPlan(combinedPlan.ToRestoreFiles(true), identity)
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
