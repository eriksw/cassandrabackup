// Copyright (c) 2019, RetailNext, Inc.
// This material contains trade secrets and confidential information of
// RetailNext, Inc.  Any use, reproduction, disclosure or dissemination
// is strictly prohibited without the explicit written permission
// of RetailNext, Inc.
// All rights reserved.

package restore

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	Cmd = kingpin.Command("restore", "")

	HostCmd    = Cmd.Command("host", "Restore this host from backup")
	ClusterCmd = Cmd.Command("cluster", "Download from multiple hosts' backups")
	SpecialCmd = Cmd.Command("special", "Special WIP stuff")

	hostCmdDryRun            = HostCmd.Flag("dry-run", "Don't actually download files").Bool()
	hostCmdAllowChangedFiles = HostCmd.Flag("allow-changed", "Allow restoration of files that changed between manifests").Bool()
	hostCmdNotBefore         = HostCmd.Flag("not-before", "Ignore manifests before this time (unix seconds)").Int64()
	hostCmdNotAfter          = HostCmd.Flag("not-after", "Ignore manifests after this time (unix seconds)").Int64()
	hostCmdCluster           = HostCmd.Flag("cluster", "Use a different cluster name when selecting a backup to restore.").String()
	hostCmdHostname          = HostCmd.Flag("hostname", "Use a specific hostname when selecting a backup to restore.").String()
	hostCmdHostnamePattern   = HostCmd.Flag("hostname-pattern", "Use a prefix pattern when selecting a backup to restore.").String()

	clusterCmdDryRun          = ClusterCmd.Flag("dry-run", "Don't actually download files").Bool()
	clusterCmdTargetDirectory = ClusterCmd.Flag("target", "A subdirectory will be created under this for each host.").Required().String()
	clusterCmdNotBefore       = ClusterCmd.Flag("not-before", "Ignore manifests before this time (unix seconds)").Int64()
	clusterCmdNotAfter        = ClusterCmd.Flag("not-after", "Ignore manifests after this time (unix seconds)").Int64()
	clusterCmdCluster         = ClusterCmd.Flag("cluster", "Download files for hosts in this cluster").Required().String()
	clusterCmdHostnamePattern = ClusterCmd.Flag("hostname-pattern", "Download for hosts matching this prefix.").Required().String()
	clusterCmdTables          = ClusterCmd.Flag("table", "Download files for these tables (keyspace.table)").Required().Strings()
	clusterCmdSkipIndexes     = ClusterCmd.Flag("skip-indexes", "Skip downloading indexes").Default("True").Bool()

	specialCmdDownloadToStaging = SpecialCmd.Flag("download-to-staging", "Download files to staging.").Bool()
	specialCmdTargetDirectory   = SpecialCmd.Flag("target", "Backups will be restored to this location.").Default("/var/lib/cassandra/data").String()
	specialCmdLinkToTarget      = SpecialCmd.Flag("link-to-target", "Link downloaded files from staging to the target.").Bool()
	specialCmdClusterMode       = SpecialCmd.Flag("all-nodes", "Download sstables from all nodes.").Bool()
	specialCmdKeyspace          = SpecialCmd.Flag("keyspace", "Restore only these keyspaces.").Strings()
	specialCmdIgnoreIncremental = SpecialCmd.Flag("ignore-incremental", "Don't restore from incremental backups.").Bool()
	specialCmdIgnoreSnapshots   = SpecialCmd.Flag("ignore-snapshots", "Don't restore from snapshot backups.").Bool()
	specialCmdIgnoreIncomplete  = SpecialCmd.Flag("ignore-incomplete", "Don't restore from incomplete backups.").Bool()
	specialCmdAllowChangedFiles = SpecialCmd.Flag("allow-changed", "Allow restoration of files that changed between manifests").Bool()
	specialCmdNotBefore         = SpecialCmd.Flag("not-before", "Ignore manifests before this time (unix seconds)").Int64()
	specialCmdNotAfter          = SpecialCmd.Flag("not-after", "Ignore manifests after this time (unix seconds)").Int64()
	specialCmdCluster           = SpecialCmd.Flag("cluster", "Use a different cluster name when selecting a backup to restore.").String()
	specialCmdHostname          = SpecialCmd.Flag("hostname", "Use a specific hostname when selecting a backup to restore.").String()
	specialCmdHostnamePattern   = SpecialCmd.Flag("hostname-pattern", "Use a prefix pattern when selecting a backup to restore.").String()
)
