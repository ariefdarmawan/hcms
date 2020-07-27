package hcms_test

import (
	"os"
	"path/filepath"
	"testing"

	"git.kanosolution.net/kano/dbflex"
	"github.com/ariefdarmawan/datahub"
	_ "github.com/ariefdarmawan/flexmgo"
	"github.com/ariefdarmawan/hcms/hcms"
	"github.com/eaciit/toolkit"
	"github.com/smartystreets/goconvey/convey"
	cv "github.com/smartystreets/goconvey/convey"
)

var (
	fileloc = "10kdata.xlsx"
	connTxt = "mongodb://localhost:27017/agsdb"
)

func init() {
	toolkit.SetTagName("json")
}

func TestImportXlsx(t *testing.T) {
	db := prepareHub(connTxt)
	defer db.Close()

	wd, _ := os.Getwd()
	xlsxLoc := filepath.Join(wd, "..", "data", fileloc)

	convey.Convey("load excel", t, func() {
		im, e := hcms.NewImporter(xlsxLoc)
		convey.So(e, convey.ShouldBeNil)

		convey.Convey("get sheet name", func() {
			sns, e := im.GetSheetNames()
			convey.So(e, convey.ShouldBeNil)
			convey.Printf("\nSheets: %s\n", toolkit.JsonString(sns))

			convey.Convey("get legal entities, validate and import the data", func() {
				// clear the table tmp just for easy checking
				db.Execute(dbflex.From("tmpXls").Delete(), nil)

				// setup the function
				adv := hcms.NewAdvEmpImport(db)
				im.OnBeforeProcessing = adv.PreProcess
				im.ImportData = adv.Import

				e = im.Import("Sheet2", 1)
				cv.So(e, cv.ShouldBeNil)

				cv.Println("")
				for k, v := range adv.RawData() {
					cv.Println("Legal entity:", k, " data member:", len(v))
				}
			})
		})
	})
}

func prepareHub(txt string) *datahub.Hub {
	h := datahub.NewHub(func() (dbflex.IConnection, error) {
		c, e := dbflex.NewConnectionFromURI(txt, nil)
		if e != nil {
			return nil, e
		}
		if e = c.Connect(); e != nil {
			return nil, e
		}
		c.SetFieldNameTag("json")
		c.SetKeyNameTag("key")

		return c, nil
	}, true, 10)
	return h
}
