package logger_test

import (
	"bytes"
	agentlogger "github.com/cloudfoundry/bosh-agent/bootstrapper/logger"
	"github.com/cloudfoundry/bosh-agent/internal/github.com/cloudfoundry/bosh-utils/logger"
	. "github.com/cloudfoundry/bosh-agent/internal/github.com/onsi/ginkgo"
	. "github.com/cloudfoundry/bosh-agent/internal/github.com/onsi/gomega"
	"os"
	"syscall"
)

var _ = Describe("Signalable logger debug", func() {
	Describe("when SIGSEGV is recieved", func() {
		It("it dumps all goroutines to stderr", func() {
			errBuf := bytes.NewBufferString("")
			outBuf := bytes.NewBufferString("")
			signalChannel := make(chan os.Signal, 1)
			writerLogger := logger.NewWriterLogger(logger.LevelError, outBuf, errBuf)
			_, doneChannel := agentlogger.NewSignalableLogger(writerLogger, signalChannel)

			signalChannel <- syscall.SIGSEGV
			<-doneChannel

			Expect(errBuf).To(ContainSubstring("Dumping goroutines"))
			Expect(errBuf).To(ContainSubstring("goroutine 5 [syscall]"))
		})
	})
})
