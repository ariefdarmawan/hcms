package hcms

import (
	model "eaciit/hcm/library/model/client"
	"fmt"
	"strings"

	"git.kanosolution.net/kano/appkit"
	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/koloni/crowd"
	"github.com/ariefdarmawan/datahub"
	"github.com/eaciit/toolkit"
)

type advEmpImport struct {
	rawDataByLEs map[string][]toolkit.M
	logger       *toolkit.LogEngine
	h            *datahub.Hub
	validateMap  map[string]map[string]map[string]string
}

func NewAdvEmpImport(h *datahub.Hub) *advEmpImport {
	a := new(advEmpImport)
	a.h = h
	a.validateMap = map[string]map[string]map[string]string{}
	return a
}

func (o *advEmpImport) PreProcess(im *Importer, rows []toolkit.M) error {
	// get data and group by LE
	les := map[string][]toolkit.M{}
	e := crowd.FromSlice(rows).
		Group(func(m toolkit.M) string {
			return m.GetString("LegalEntity")
		}).CollectMap().Run(&les)
	if e != nil {
		return e
	}
	o.rawDataByLEs = les

	// prepare validation-map for each LE
	fieldMaps := []string{"EmployeeCode", "OfficialEmailID"}
	for k := range o.rawDataByLEs {
		mvle := map[string]map[string]string{}
		emps := []toolkit.M{}
		qparm := dbflex.NewQueryParam().
			SetWhere(dbflex.And(dbflex.Eq("Status", "list"), dbflex.Eq("LegalEntity.LegalEntityId", k))).
			SetSelect(fieldMaps...)
		if e = o.h.PopulateByParm(new(model.EmployeeData).TableName(), qparm, &emps); e != nil {
			return fmt.Errorf("fail get validation map. %s", e.Error())
		}

		for _, f := range fieldMaps {
			mvle[f] = map[string]string{}
		}

		for _, emp := range emps {
			ecode := emp.GetString("EmployeeCode")
			for _, f := range fieldMaps {
				v := strings.ToLower(emp.GetString(f))
				if v != "" {
					mvle[f][v] = ecode
				}
			}
		}

		// TODO: prepare other validation map if any
		o.validateMap[k] = mvle
	}

	return nil
}

func (o *advEmpImport) Import(im *Importer, data toolkit.M) error {
	// mandatory field checking
	ecode := data.GetString("EmployeeCode")
	leid := data.GetString("LegalEntity")
	if leid == "" {
		return fmt.Errorf("data %s has no legalentity", ecode)
	}

	// duplicate checking
	fieldMaps := []string{"EmployeeCode", "OfficialEmailID"}
	for _, f := range fieldMaps {
		if data.Has(f) {
			// TODO: check for other data type
			v := strings.ToLower(data[f].(string))
			existingEcode, found := o.validateMap[leid][f][v]
			if found && existingEcode != ecode {
				return fmt.Errorf("data %s has duplicate value with existing data on field %s value %s", ecode, f, v)
			}
		}
	}

	// TODO: run other validation

	// no duplicate then save
	// TODO: save to EmployeeData model first
	tablename := "tmpXls"
	data.Set("_id", ecode)
	o.h.SaveAny(tablename, data)

	// update validationmap and add it
	for _, f := range fieldMaps {
		if data.Has(f) {
			v := strings.ToLower(data.GetString(f))
			o.validateMap[leid][f][v] = ecode
		}
	}
	return nil
}

func (o *advEmpImport) RawData() map[string][]toolkit.M {
	return o.rawDataByLEs
}

func (o *advEmpImport) SetLogger(l *toolkit.LogEngine) *advEmpImport {
	o.logger = l
	return o
}

func (o *advEmpImport) Logger() *toolkit.LogEngine {
	if o.logger == nil {
		o.logger = appkit.Log()
	}
	return o.logger
}
