package storage

import "fmt"

type BgMetadataStorageConnector interface {
	UpdateMetricMetadata(metric *Metric) error
	InsertDirectory(dir *MetricDirectory) error
	SelectDirectory(dir string) (string, error) // SelectDirectory returns the parent directory or an error if it is not created
	// empty string means root directory
}

// default connector, does nothing used for testing
type BgMetadataNoOpStorageConnector struct {
	UpdatedDirectories  []string
	SelectedDirectories []string
	UpdatedMetrics      []string
}

func (cc *BgMetadataNoOpStorageConnector) UpdateMetricMetadata(metric *Metric) error {
	cc.UpdatedMetrics = append(cc.UpdatedMetrics, metric.name)
	return nil
}

func (cc *BgMetadataNoOpStorageConnector) InsertDirectory(dir *MetricDirectory) error {
	if dir.name != "" {
		cc.UpdatedDirectories = append(cc.UpdatedDirectories, dir.name)
	}
	return nil
}

func (cc *BgMetadataNoOpStorageConnector) SelectDirectory(dir string) (string, error) {
	cc.SelectedDirectories = append(cc.SelectedDirectories, dir)
	return dir, fmt.Errorf("Directory not found")
}
