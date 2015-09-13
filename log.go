package rabric

import (
	"github.com/op/go-logging"
	glog "log"
	"os"
	// "fmt"
	// "io/ioutil"
)

var (
	logFlags = glog.Ldate | glog.Ltime | glog.Lshortfile
	log      Logger
)

// Logger is an interface compatible with log.Logger.
type Logger interface {
	Println(v ...interface{})
	Printf(format string, v ...interface{})
}

type noopLogger struct{}

func (n noopLogger) Println(v ...interface{}) {

}

func (n noopLogger) Printf(format string, v ...interface{}) {
	// out.Notice(fmt.Sprintf(format, v...))
}

// Check out their github page for more info on the coloring
var format = logging.MustStringFormatter(
	"%{color}[%{longfunc}]  %{message}",
	// "[%{color}%{time:15:04:05.000} %{longfunc}]  %{message}",
)

func Log() {
	// log = glog.New(os.Stderr, "", logFlags)

	// For demo purposes, create two backend for os.Stderr.
	backend1 := logging.NewLogBackend(os.Stderr, "", 0)
	// backend2 := logging.NewLogBackend(os.Stderr, "", 0)

	// For messages written to backend2 we want to add some additional
	// information to the output, including the used log level and the name of
	// the function.
	formatter := logging.NewBackendFormatter(backend1, format)

	// Only errors and more severe messages should be sent to backend1
	backend1Leveled := logging.AddModuleLevel(backend1)

	if os.Getenv("DEBUG") != "" {
		backend1Leveled.SetLevel(logging.DEBUG, "")
		// log = glog.New(ioutil.Discard, "", logFlags)
		log = noopLogger{}
	} else {
		backend1Leveled.SetLevel(logging.CRITICAL, "")
		log = noopLogger{}
	}

	// Set the backends to be used.
	logging.SetBackend(backend1Leveled, formatter)

	// out.Debug("debug %s", Password("secret"))
	// out.Info("info")
	// out.Notice("notice")
	// out.Warning("warning")
	// out.Error("err")
	// out.Critical("crit")
}

func SetLogger(l Logger) {
	log = l
}

func logErr(err error) error {
	if err == nil {
		return nil
	}
	if l, ok := log.(*glog.Logger); ok {
		l.Output(2, err.Error())
	} else {
		//log.Println(err)
	}
	return err
}

// New Logging implementation
var out = logging.MustGetLogger("example")

// Password is just an example type implementing the Redactor interface. Any
// time this is logged, the Redacted() function will be called.
type Password string
