package config

import (
	"fmt"
	"os"

	"go.yaml.in/yaml/v3"
)

func Load(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read configuration: %w", err)
	}

	if err := yaml.Unmarshal(data, value); err != nil {
		return fmt.Errorf("decode configuration: %w", err)
	}

	return nil
}
