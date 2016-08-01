package devicepathresolver

import (
	"fmt"
	"strings"
	"time"
	"regexp"

	boshopeniscsi "github.com/cloudfoundry/bosh-agent/platform/openiscsi"
	boshsettings "github.com/cloudfoundry/bosh-agent/settings"
	bosherr "github.com/cloudfoundry/bosh-utils/errors"
	boshlog "github.com/cloudfoundry/bosh-utils/logger"
	boshsys "github.com/cloudfoundry/bosh-utils/system"
)

// MultipathISCSIDevicePathResolver resolves device path by performing Open-iscsi discovery
type MultipathISCSIDevicePathResolver struct {
	diskWaitTimeout time.Duration
	runner boshsys.CmdRunner
	openiscsi	boshopeniscsi.OpenIscsi
	fs              boshsys.FileSystem
	logTag string
	logger boshlog.Logger
}

func NewMultipathIscsiDevicePathResolver(
	diskWaitTimeout time.Duration,
        runner boshsys.CmdRunner,
        openiscsi	boshopeniscsi.OpenIscsi,
	fs boshsys.FileSystem,
	logger boshlog.Logger,
) MultipathISCSIDevicePathResolver {
	return MultipathISCSIDevicePathResolver{
		diskWaitTimeout: diskWaitTimeout,
		runner:  runner,
		openiscsi: openiscsi,
		fs:              fs,
		logTag: "multipathiscsiresolver",
		logger: logger,
	}
}

func (midpr MultipathISCSIDevicePathResolver) GetRealDevicePath(diskSettings boshsettings.DiskSettings) (string, bool, error) {
	if diskSettings.Initiatorname == "" {
		return "", false, bosherr.Errorf("ISCSI Initiatorname is not set")
	}

	if diskSettings.Username == "" {
		return "", false, bosherr.Errorf("ISCSI Username is not set")
	}

	if diskSettings.Ipaddress == "" {
		return "", false, bosherr.Errorf("ISCSI Iface Ipaddress is not set")
	}

	if diskSettings.Password == "" {
		return "", false, bosherr.Errorf("ISCSI Password is not set")
	}

	existingPaths :=[]string{}

	result, _, _, err := midpr.runner.RunCommand("dmsetup", "ls")
	if err != nil {
		return "", false, bosherr.WrapError(err, "Could not determining device mapper entries")
	}

	lines := strings.Split(strings.Trim(result, "\n"), "\n")
	for _, line :=range lines {
		if match, _ := regexp.MatchString("-part1", line); match {
			exitingPath := path.Join("/dev/mapper", strings.Fields(line)[0])
			existingPaths = append(existingPaths, exitingPath)
		}
	}

	if len(existingPaths) >= 2 {
		return "", false, bosherr.WrapError(err, "More than 2 persistent disks attached")
	}

	err = midpr.openiscsi.Setup()
	if err != nil {
		return "", false, bosherr.WrapError(err, "Could not setup Open-iscsi")
	}

	err = midpr.openiscsi.Discovery(diskSettings.Ipaddress)
	if err != nil {
		return "", false, bosherr.WrapError(err, fmt.Sprintf("Could not discovery lun against portal %s", diskSettings.Ipaddress))
	}

	err = midpr.openiscsi.Login()
	if err != nil {
		return "", false, bosherr.WrapError(err, "Could not login all sessions")
	}

	stopAfter := time.Now().Add(midpr.diskWaitTimeout)

	for {
		midpr.logger.Debug(midpr.logTag, "Waiting for device to appear")

		if time.Now().After(stopAfter) {
			return "", true, bosherr.Errorf("Timed out getting real device path by portal '%s'", diskSettings.Ipaddress)
		}

		time.Sleep(100 * time.Millisecond)

		result, _, _, err := midpr.runner.RunCommand("dmsetup", "ls")
		if err != nil {
			return "", false, bosherr.WrapError(err, "Could not determining device mapper entries")
		}

		lines := strings.Split(strings.Trim(result, "\n"), "\n")
		for _, line :=range lines {
			if match, _ := regexp.MatchString("-part1", line); match {
				matchedPath := path.Join("/dev/mapper", strings.Fields(line)[0])

				if len(existingPaths) == 0 {
					midpr.logger.Debug(midpr.logTag, "Found real path '%s'", matchedPath)
					return matchedPath, false, nil
				}

				for _, existingPath := range existingPaths {
					if strings.EqualFold(matchedPath, existingPath) {
						continue
					} else {
						midpr.logger.Debug(midpr.logTag, "Found real path '%s'", matchedPath)
						return matchedPath, false, nil
					}
				}
			}
		}
	}
}
