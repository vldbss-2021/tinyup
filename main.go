package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	scheduler = "tinyscheduler-server"
	tinykv    = "tinykv-server"
	tinysql   = "tidb-server"
)

var (
	rootCmd = &cobra.Command{
		Use:   "tinyup subcommand",
		Short: "simple cluster operation tool",
	}

	nodeNumber    int
	deployPath    string
	binaryPath    string
	schedulerPort = 2379
	kvPort        = 20160
	dbPort        = 4000

	deployCmd = &cobra.Command{
		Use:   "deploy",
		Short: "deploy a local cluster",
		Run: func(cmd *cobra.Command, args []string) {
			// Create the deploy path.
			if _, err := os.Stat(deployPath); os.IsNotExist(err) {
				err := os.Mkdir(deployPath, os.ModePerm)
				if err != nil {
					log.Fatal("create deploy dir failed", zap.Error(err))
				}
			} else {
				log.Fatal("the dir already exists, please remove it first", zap.String("deploy path", deployPath))
			}

			// Create the scheduler and kv path, and copy new binaries to the target path.
			schedulerPath := path.Join(deployPath, "scheduler")
			err := os.Mkdir(schedulerPath, os.ModePerm)
			if err != nil {
				log.Fatal("create scheduler dir failed", zap.Error(err))
			}
			_, err = exec.Command("cp", path.Join(binaryPath, scheduler), schedulerPath).Output()
			if err != nil {
				log.Fatal("copy scheduler binary to path failed", zap.Error(err),
					zap.String("binaryPath", binaryPath), zap.String("schedulerPath", schedulerPath))
			}
			for i := 0; i < nodeNumber; i++ {
				kvPath := path.Join(deployPath, fmt.Sprintf("tinykv%d", i))
				err = os.Mkdir(kvPath, os.ModePerm)
				if err != nil {
					log.Fatal("create tinykv dir failed", zap.Error(err))
				}
				_, err = exec.Command("cp", path.Join(binaryPath, tinykv), kvPath).Output()
				if err != nil {
					log.Fatal("copy tinykv binary to path failed", zap.Error(err),
						zap.String("binaryPath", binaryPath), zap.String("tinykvPath", kvPath))
				}
			}

			tinysqlPath := path.Join(deployPath, "tinysql")
			err = os.Mkdir(tinysqlPath, os.ModePerm)
			if err != nil {
				log.Fatal("create tinysql dir failed", zap.Error(err))
			}
			_, err = exec.Command("cp", path.Join(binaryPath, tinysql), tinysqlPath).Output()
			if err != nil {
				log.Fatal("copy tinysql binary to path failed", zap.Error(err),
					zap.String("binaryPath", binaryPath), zap.String("tinysqlPath", tinysqlPath))
			}
		},
	}

	startCmd = &cobra.Command{
		Use:   "start",
		Short: "start the cluster",
		Run: func(cmd *cobra.Command, args []string) {
			log.Info("start info",
				zap.Int("scheduler port", schedulerPort),
				zap.Int("kv port", kvPort),
				zap.Int("db port", dbPort))

			// Try to start the scheduler server.
			_, err := Check(schedulerPort)
			if err != nil {
				log.Fatal("check scheduler port failed", zap.Error(err), zap.Int("port", schedulerPort))
			}
			schedulerPath := path.Join(deployPath, "scheduler")
			t := time.Now()
			tstr := t.Format("20060102150405")
			logName := fmt.Sprintf("log_%s", tstr)
			startSchedulerCmd := fmt.Sprintf("nohup ./%s > %s 2>&1 &", scheduler, logName)
			log.Info("start scheduler cmd", zap.String("cmd", startSchedulerCmd))
			shellCmd := exec.Command("bash", "-c", startSchedulerCmd)
			shellCmd.Dir = schedulerPath
			_, err = shellCmd.Output()
			if err != nil {
				os.Remove(path.Join(schedulerPath, logName))
				log.Fatal("start scheduler failed", zap.Error(err))
			}
			err = waitPortUse([]int{schedulerPort})
			if err != nil {
				log.Fatal("wait scheduler port in use error", zap.Error(err))
			}

			// Try to start the tinykv server.
			ports := make([]int, 0, nodeNumber)
			for i := 0; i < nodeNumber; i++ {
				kvPath := path.Join(deployPath, fmt.Sprintf("tinykv%d", i))
				thisKVPort := kvPort + i
				_, err := Check(thisKVPort)
				if err != nil {
					log.Fatal("check kv port failed", zap.Error(err), zap.Int("kv port", thisKVPort))
				}
				startKvCmd := fmt.Sprintf(`nohup ./%s > %s --path . --addr "127.0.0.1:%d" 2>&1 &`, tinykv, logName, thisKVPort)
				log.Info("start tinykv cmd", zap.String("cmd", startKvCmd))
				shellCmd := exec.Command("bash", "-c", startKvCmd)
				shellCmd.Dir = kvPath
				_, err = shellCmd.Output()
				if err != nil {
					os.Remove(path.Join(kvPath, logName))
					log.Fatal("start scheduler failed", zap.Error(err))
				}
				ports = append(ports, thisKVPort)
			}
			err = waitPortUse(ports)
			if err != nil {
				log.Fatal("wait tinykv port in use error", zap.Error(err))
			}

			// Try to start the scheduler server.
			_, err = Check(dbPort)
			if err != nil {
				log.Fatal("check db port failed", zap.Error(err), zap.Int("port", dbPort))
			}
			dbPath := path.Join(deployPath, "tinysql")
			t = time.Now()
			tstr = t.Format("20060102150405")
			logName = fmt.Sprintf("log_%s", tstr)
			startDBCmd := fmt.Sprintf(`nohup ./%s -store tikv -path 127.0.0.1:%d > %s 2>&1 &`, tinysql, schedulerPort, logName)
			log.Info("start scheduler cmd", zap.String("cmd", startDBCmd))
			shellCmd = exec.Command("bash", "-c", startDBCmd)
			shellCmd.Dir = dbPath
			_, err = shellCmd.Output()
			if err != nil {
				os.Remove(path.Join(dbPath, logName))
				log.Fatal("start scheduler failed", zap.Error(err))
			}
			err = waitPortUse([]int{dbPort})
			if err != nil {
				log.Fatal("wait db port in use error", zap.Error(err))
			}

			log.Info("cluster started")
		},
	}

	stopCmd = &cobra.Command{
		Use:   "stop",
		Short: "stop the cluster",
		Run: func(cmd *cobra.Command, args []string) {
			// Try to stop the scheduler server.
			killScheduler := fmt.Sprintf("pkill -f %s", scheduler)
			log.Info("stop scheduler cmd", zap.String("cmd", killScheduler))
			shellCmd := exec.Command("bash", "-c", killScheduler)
			_, err := shellCmd.Output()
			if err != nil {
				log.Fatal("stop scheduler failed", zap.Error(err))
			}
			err = waitPortFree([]int{schedulerPort})
			if err != nil {
				log.Fatal("wait scheduler port free error", zap.Error(err))
			}

			// Try to stop the tinykv servers.
			killKvServer := fmt.Sprintf("pkill -f %s", tinykv)
			log.Info("stop tinykv cmd", zap.String("cmd", killKvServer))
			shellCmd = exec.Command("bash", "-c", killKvServer)
			_, err = shellCmd.Output()
			if err != nil {
				log.Fatal("stop tinykv failed", zap.Error(err))
			}
			ports := make([]int, 0, nodeNumber)
			for i := 0; i < nodeNumber; i++ {
				thisKVPort := kvPort + i
				ports = append(ports, thisKVPort)
			}
			err = waitPortFree(ports)
			if err != nil {
				log.Fatal("wait tinykv port free error", zap.Error(err))
			}

			// Try to stop the tinysql servers.
			killDB := fmt.Sprintf("pkill -f %s", tinysql)
			log.Info("stop tinysql cmd", zap.String("cmd", killDB))
			shellCmd = exec.Command("bash", "-c", killDB)
			_, err = shellCmd.Output()
			if err != nil {
				log.Fatal("stop db failed", zap.Error(err))
			}
			err = waitPortFree([]int{dbPort})
			if err != nil {
				log.Fatal("wait db port free error", zap.Error(err))
			}
		},
	}

	upgradeCmd = &cobra.Command{
		Use:   "upgrade",
		Short: "upgrade the cluster",
		Run: func(cmd *cobra.Command, args []string) {
			// Check the cluster is stopped.
			err := waitPortFree([]int{schedulerPort})
			if err != nil {
				log.Fatal("wait scheduler port free error", zap.Error(err))
			}
			ports := make([]int, 0, nodeNumber)
			for i := 0; i < nodeNumber; i++ {
				thisKVPort := kvPort + i
				ports = append(ports, thisKVPort)
			}
			err = waitPortFree(ports)
			if err != nil {
				log.Fatal("wait tinykv port free error", zap.Error(err))
			}
			err = waitPortFree([]int{dbPort})
			if err != nil {
				log.Fatal("wait db port free error", zap.Error(err))
			}

			// Substitute the binary.
			schedulerPath := path.Join(deployPath, "scheduler")
			_, err = exec.Command("cp", path.Join(binaryPath, scheduler), schedulerPath).Output()
			if err != nil {
				log.Fatal("copy scheduler binary to path failed", zap.Error(err),
					zap.String("binaryPath", binaryPath), zap.String("schedulerPath", schedulerPath))
			}
			for i := 0; i < nodeNumber; i++ {
				kvPath := path.Join(deployPath, fmt.Sprintf("tinykv%d", i))
				_, err = exec.Command("cp", path.Join(binaryPath, tinykv), kvPath).Output()
				if err != nil {
					log.Fatal("copy tinykv binary to path failed", zap.Error(err),
						zap.String("binaryPath", binaryPath), zap.String("kvPath", kvPath))
				}
			}
			tinysqlPath := path.Join(deployPath, "tinysql")
			_, err = exec.Command("cp", path.Join(binaryPath, tinysql), tinysqlPath).Output()
			if err != nil {
				log.Fatal("copy tinysql binary to path failed", zap.Error(err),
					zap.String("binaryPath", binaryPath), zap.String("tinysqlPath", tinysqlPath))
			}
		},
	}

	destroyCmd = &cobra.Command{
		Use:   "destroy",
		Short: "destroy the cluster",
		Run: func(cmd *cobra.Command, args []string) {
			log.Info("destroy is starting to remove the whole deployed directory",
				zap.String("deployPath", deployPath))
			err := os.RemoveAll(deployPath)
			if err != nil {
				log.Fatal("remove the deploy path failed", zap.Error(err))
			}
			log.Info("cleanup finished")
		},
	}
)

// Check if a port is available
func Check(port int) (status bool, err error) {

	// Concatenate a colon and the port
	host1 := ":" + strconv.Itoa(port)
	host2 := "127.0.0.1:" + strconv.Itoa(port)

	// Try to create a server with the port
	server1, err1 := net.Listen("tcp", host1)
	if err1 == nil {
		server1.Close()
	}
	server2, err2 := net.Listen("tcp", host2)
	if err2 == nil {
		server2.Close()
	}

	if err1 != nil || err2 != nil {
		if err1 != nil {
			return false, err1
		}
		return false, err2
	}

	// we successfully used and closed the port
	// so it's now available to be used again
	return true, nil

}

const waitDurationLimit = time.Duration(2 * time.Minute)
const waitSleepDuration = time.Duration(300 * time.Millisecond)

func checkLoop(waitPorts []int, checkFn func([]int) bool) error {
	waitStart := time.Now()
	for {
		if time.Since(waitStart) > waitDurationLimit {
			log.Error("check port timeout")
			return errors.New("check port timeout")
		}
		if checkFn(waitPorts) {
			break
		}
		time.Sleep(waitSleepDuration)
	}
	return nil
}

func waitPortFree(waitPorts []int) error {
	return checkLoop(waitPorts, func(ports []int) bool {
		allFree := true
		for _, port := range waitPorts {
			_, err := Check(port)
			if err != nil {
				allFree = false
				break
			}
		}
		return allFree
	})
}

func waitPortUse(waitPorts []int) error {
	time.Sleep(100 * time.Millisecond)
	return checkLoop(waitPorts, func(ports []int) bool {
		allInUse := true
		for _, port := range waitPorts {
			_, err := Check(port)
			if err == nil {
				allInUse = false
				break
			}
		}
		return allInUse
	})
}

func init() {
	rootCmd.Flags().IntVarP(&nodeNumber, "num", "n", 3, "the number of the tinykv servers")
	rootCmd.Flags().StringVarP(&deployPath, "deploy_path", "d", "./bin/deploy", "the deploy path")
	rootCmd.Flags().StringVarP(&binaryPath, "binary_path", "b", "./bin", "the binary path")

	rootCmd.AddCommand(deployCmd)
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(stopCmd)
	rootCmd.AddCommand(upgradeCmd)
	rootCmd.AddCommand(destroyCmd)
}

func main() {
	rootCmd.Execute()
}
