package config

import (
	"os"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/viper"
)

func LoadConfig(config any, defaultName string) error {
	configDir, exists := os.LookupEnv("CONFIG_DIR")
	if !exists {
		configDir = "resources/"
	}
	configFileName, exists := os.LookupEnv("CONFIG_NAME")
	if !exists {
		configFileName = defaultName
	}
	configFileName = strings.ToLower(configFileName)
	err := LoadConfigFromPath(configDir, configFileName, config)
	if err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"configDir":      configDir,
				"configFileName": configFileName,
			}).Fatal("Couldn't load config")
		return err
	} else {
		log.WithFields(log.Fields{
			"configDir":      configDir,
			"configFileName": configFileName,
		}).Info("Successfully loaded config")
	}
	return nil
}

func LoadConfigFromPath(configDir, configFileName string, config any) error {
	initViper(configDir, configFileName)
	err := viper.ReadInConfig()
	if err != nil {
		return err
	}
	err = viper.Unmarshal(config)
	if err != nil {
		return err
	}
	return nil
}

func initViper(configDir, configFileName string) {
	viper.SetConfigName(configFileName)
	viper.SetConfigType("yaml")
	viper.AddConfigPath(configDir)
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
}
