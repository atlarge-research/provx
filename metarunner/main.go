package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"

	"github.com/gurkankaymak/hocon"
	"github.com/rs/xid"
)

type MagpieConfig struct {
	JobId             string
	NodeCount         int
	JobScriptLocation string
}

type JobEnvironment struct {
	ExperimentConfigPath string
}

func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	// input: metarunner config, outputDir
	scriptLocationFlag := flag.String("outputDir", "/home/gmo520/provxlib/metarunner/scripts", "location to write launch scripts to")
	configLocationFlag := flag.String("configDir", "/home/gmo520/provxlib/metarunner/configs", "location of experiment scenarios")

	flag.Parse()

	scriptLocationPrefix, err := filepath.Abs(*scriptLocationFlag)
	fatal(err)
	configLocationPrefix, err := filepath.Abs(*configLocationFlag)
	fatal(err)

	err = filepath.Walk(configLocationPrefix, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		configurationName := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		fmt.Println(configurationName)

		conf, err := hocon.ParseResource(path)
		if err != nil {
			log.Fatal("error while parsing configuration: ", err)
		}
		workerNodes := conf.GetArray("cluster.workerNodes")
		fmt.Println(workerNodes)

		for _, value := range workerNodes {
			workerCount, err := strconv.Atoi(value.String())
			if err != nil {
				log.Fatal("error while parsing configuration: ", err)
			}

			guid := xid.New()
			jobScriptLocation := fmt.Sprintf("%s/job-script-%s.sh", scriptLocationPrefix, configurationName)
			jobId := fmt.Sprintf("provx-%s", guid.String())

			// take into account YARN master node and driver job
			config := MagpieConfig{jobId, workerCount + 2, jobScriptLocation}

			t, err := template.ParseFiles("templates/magpie.sbatch-srun-provx-with-yarn-and-hdfs")
			// t, err := template.ParseFiles("templates/magpie.sbatch-srun-provx")
			fatal(err)

			// Generate sbatch Magpie job script
			filename := fmt.Sprintf("%s/magpie.sbatch-srun-provx-%s-%02d.sh", scriptLocationPrefix, configurationName, value)
			f, err := os.Create(filename)
			fatal(err)

			err = t.Execute(f, config)
			fatal(err)

			// Create launch script (if not exists)
			if _, err := os.Stat(jobScriptLocation); errors.Is(err, os.ErrNotExist) {
				t, err = template.ParseFiles("templates/launch.sh")
				fatal(err)

				f, err := os.Create(jobScriptLocation)
				fatal(err)

				magpieConfig := JobEnvironment{path}
				err = t.Execute(f, magpieConfig)
				fatal(err)

				err = os.Chmod(jobScriptLocation, 0775)
				fatal(err)
			}
		}
		// fmt.Println(path, info.Size())
		return nil
	})
	fatal(err)
}
