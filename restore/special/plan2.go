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

package special

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/restore/plan"
)

type TableComponents map[VersionComponent]digest.ForRestore

func (tc TableComponents) add(pn FileName, forRestore digest.ForRestore) {
	vc := VersionComponent{
		Version:   pn.Version,
		Component: pn.Component,
	}
	tc[vc] = forRestore
}

func (tc TableComponents) DataDigest() (digest.ForRestore, bool) {
	for vc, forRestore := range tc {
		if vc.Component == "Data.db" {
			return forRestore, true
		}
	}
	return digest.ForRestore{}, false
}

type ComponentsByGeneration map[int]TableComponents

func (cbg ComponentsByGeneration) add(pn FileName, forRestore digest.ForRestore) {
	components := cbg[pn.Generation]
	if components == nil {
		components = make(TableComponents)
	}
	components.add(pn, forRestore)
	cbg[pn.Generation] = components
}

type VersionComponent struct {
	Version   string
	Component string
}

func (vc VersionComponent) String() string {
	return vc.Version + "-" + vc.Component
}

func (vc VersionComponent) FileName(generation int) string {
	return vc.Version + "-" + strconv.Itoa(generation) + "-big-" + vc.Component
}

type Location struct {
	Keyspace string
	Table    string
	Index    string
}

func (l Location) String() string {
	if l.Index != "" {
		return l.Keyspace + "/" + l.Table + "/" + l.Index
	}
	return l.Keyspace + "/" + l.Table
}

type ComponentsByGenerationByLocation map[Location]ComponentsByGeneration

func (cbgl ComponentsByGenerationByLocation) add(pn FileName, forRestore digest.ForRestore) {
	loc := Location{
		Keyspace: pn.Keyspace,
		Table:    pn.Table,
		Index:    pn.Index,
	}
	cbg := cbgl[loc]
	if cbg == nil {
		cbg = make(ComponentsByGeneration)
	}
	cbg.add(pn, forRestore)
	cbgl[loc] = cbg
}

type Plan2 struct {
	FromNodePlan  ComponentsByGenerationByLocation
	FromLiveFiles ComponentsByGenerationByLocation

	Result map[Location]LocationPlan
}

type LocationPlan struct {
	GenerationByData map[digest.ForRestore]int
	Generations      map[int]ComponentsPlan
	Warnings         []string
}

func (lp *LocationPlan) addExisting(existing ComponentsByGeneration) {
	if lp.GenerationByData == nil {
		lp.GenerationByData = make(map[digest.ForRestore]int)
	}
	if lp.Generations == nil {
		lp.Generations = make(map[int]ComponentsPlan)
	}

	for generation, components := range existing {
		dd, ok := components.DataDigest()
		if !ok {
			msg := fmt.Sprintf("existing generation without data: %d: %+v", generation, components)
			lp.Warnings = append(lp.Warnings, msg)
			// put something into Generations to mark the number as unavailable
			lp.Generations[generation] = ComponentsPlan{}
			continue
		}
		lp.GenerationByData[dd] = generation

		cp := lp.Generations[generation]
		// defensive copy because ???
		cp.Existing = make(TableComponents)
		for vc, forRestore := range components {
			cp.Existing[vc] = forRestore
		}
		lp.Generations[generation] = cp
	}
}

func (lp *LocationPlan) addRestore(dl ComponentsByGeneration) {
	if lp.GenerationByData == nil {
		lp.GenerationByData = make(map[digest.ForRestore]int)
	}
	if lp.Generations == nil {
		lp.Generations = make(map[int]ComponentsPlan)
	}

	var generationToTry int = 1
	for sourceGeneration, components := range dl {
		dd, ok := components.DataDigest()
		if !ok {
			msg := fmt.Sprintf("restore generation without data: %d: %+v", sourceGeneration, components)
			lp.Warnings = append(lp.Warnings, msg)
			continue
		}

		if _, ok := lp.GenerationByData[dd]; ok {
			// we already have components for this on disk in existing, skip
			continue
		}

		// find an available generation number
		targetGeneration := generationToTry
		for {
			if _, exists := lp.Generations[targetGeneration]; !exists {
				break
			}
			targetGeneration++
		}
		generationToTry = targetGeneration + 1

		cp := lp.Generations[targetGeneration]
		// defensive copy because ???
		cp.Download = make(TableComponents)
		for vc, forRestore := range components {
			cp.Download[vc] = forRestore
		}
		lp.Generations[targetGeneration] = cp
	}
}

func (lp LocationPlan) descriptionItems(download, existing bool) descriptionItems {
	var items descriptionItems
	for g, cp := range lp.Generations {
		if download {
			for vc := range cp.Download {
				item := descriptionItem{
					text:       vc.FileName(g),
					isDownload: true,
				}
				items = append(items, item)
			}
		}
		if existing {
			for vc := range cp.Existing {
				item := descriptionItem{
					text:       vc.FileName(g),
					isDownload: false,
				}
				items = append(items, item)
			}
		}
	}
	sort.Sort(items)
	return items
}

type ComponentsPlan struct {
	Existing TableComponents
	Download TableComponents
}

func CollateNodePlan(nodePlan plan.NodePlan) (result ComponentsByGenerationByLocation, unrecognized []string) {
	result = make(ComponentsByGenerationByLocation)

	for fileName, forRestore := range nodePlan.Files {
		pn, ok := parseName(fileName)
		if !ok {
			switch {
			// We don't care about these non-sstable files.
			case strings.HasSuffix(fileName, "manifest.json"):
			case strings.HasSuffix(fileName, "schema.cql"):
			default:
				unrecognized = append(unrecognized, fileName)
			}
			continue
		}
		result.add(pn, forRestore)
	}

	return
}

func CollateExistingFiles(files ToUploadByManifestPath) (result ComponentsByGenerationByLocation, unrecognized []string) {
	result = make(ComponentsByGenerationByLocation)
	for fileName, toUpload := range files {
		pn, ok := parseName(fileName)
		if !ok {
			switch {
			// We don't care about these non-sstable files.
			case strings.HasSuffix(fileName, "manifest.json"):
			case strings.HasSuffix(fileName, "schema.cql"):
			default:
				unrecognized = append(unrecognized, fileName)
			}
			continue
		}
		result.add(pn, toUpload.Digests.ForRestore())
	}
	return
}

type LocationPlans map[Location]LocationPlan

func (lp LocationPlans) ToRestoreFiles(includeExisting bool) map[string]digest.ForRestore {
	result := make(map[string]digest.ForRestore)

	for loc, locPlan := range lp {
		for generation, componentsPlan := range locPlan.Generations {
			if includeExisting {
				for component, forRestore := range componentsPlan.Existing {
					fn := FileName{
						Keyspace:   loc.Keyspace,
						Table:      loc.Table,
						Index:      loc.Index,
						Version:    component.Version,
						Generation: generation,
						Component:  component.Component,
					}
					result[fn.String()] = forRestore
				}
			}
			for component, forRestore := range componentsPlan.Download {
				fn := FileName{
					Keyspace:   loc.Keyspace,
					Table:      loc.Table,
					Index:      loc.Index,
					Version:    component.Version,
					Generation: generation,
					Component:  component.Component,
				}
				result[fn.String()] = forRestore
			}
		}
	}

	return result
}

func (lp LocationPlans) DescribeTo(w io.Writer, warnings, existing, download bool) error {
	locations := make(Locations, 0, len(lp))
	for loc := range lp {
		locations = append(locations, loc)
	}
	sort.Sort(locations)

	for _, loc := range locations {

		var wroteHeader bool
		if warnings && len(lp[loc].Warnings) > 0 {
			if _, err := fmt.Fprintf(w, "\t%s\n", loc.String()); err != nil {
				return err
			}
			wroteHeader = true
			for _, msg := range lp[loc].Warnings {
				s := "!\t\t" + msg + "\n"
				if _, err := w.Write([]byte(s)); err != nil {
					return err
				}
			}
		}
		items := lp[loc].descriptionItems(download, existing)
		if len(items) > 0 && !wroteHeader {
			if _, err := fmt.Fprintf(w, "\t%s\n", loc.String()); err != nil {
				return err
			}
		}
		for _, item := range items {
			if _, err := item.writeTo(w); err != nil {
				return err
			}
		}
	}

	return nil
}

type Locations []Location

func (ls Locations) Len() int {
	return len(ls)
}

func (ls Locations) Less(i, j int) bool {
	return ls[i].Keyspace < ls[j].Keyspace || ls[i].Table < ls[j].Table || ls[i].Index < ls[j].Index
}

func (ls Locations) Swap(i, j int) {
	ls[i], ls[j] = ls[j], ls[i]
}

func BuildLocationPlans(existing ComponentsByGenerationByLocation, nodePlans ...ComponentsByGenerationByLocation) LocationPlans {
	result := make(LocationPlans)

	if existing != nil {
		for location, cbg := range existing {
			lp := result[location]
			lp.addExisting(cbg)
			result[location] = lp
		}
	}

	for _, nodePlan := range nodePlans {
		for location, cbg := range nodePlan {
			lp := result[location]
			lp.addRestore(cbg)
			result[location] = lp
		}
	}

	return result
}
