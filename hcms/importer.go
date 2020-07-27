package hcms

import (
	"fmt"

	excel "github.com/360EntSecGroup-Skylar/excelize"
	"github.com/eaciit/toolkit"
)

type Importer struct {
	Filepath string
	xf       *excel.File

	OnBeforeProcessing func(*Importer, []toolkit.M) error
	OnProcessed        func(*Importer) error
	ImportData         func(im *Importer, data toolkit.M) error

	sheetRows map[string][]toolkit.M
}

func NewImporter(loc string) (*Importer, error) {
	i := new(Importer)
	i.Filepath = loc
	xf, err := excel.OpenFile(loc)
	if err != nil {
		return nil, err
	}
	i.xf = xf
	i.sheetRows = make(map[string][]toolkit.M)
	return i, nil
}

func (im *Importer) GetSheetNames() ([]string, error) {
	res := []string{}

	for _, name := range im.xf.GetSheetMap() {
		res = append(res, name)
	}
	return res, nil
}

func (im *Importer) Rows(sn string, headerRow int) ([]toolkit.M, error) {
	if rows, ok := im.sheetRows[sn]; ok {
		return rows, nil
	}

	xrows, _ := im.xf.GetRows(sn)
	if len(xrows) <= headerRow {
		return []toolkit.M{}, fmt.Errorf("xlsx data is empty or header row index (%d) is less than data length (%d)", headerRow, len(xrows))
	}

	rows := []toolkit.M{}
	fields := make([]string, len(xrows[headerRow-1]))
	for i, r := range xrows[headerRow-1:] {
		// prepare mtmp
		if i == 0 {
			for ci, v := range r {
				fields[ci] = v
			}
			continue
		}

		// more than 1, save the data
		mdata := toolkit.M{}
		for ci, v := range r {
			mdata[fields[ci]] = v
		}
		rows = append(rows, mdata)
	}

	im.sheetRows[sn] = rows
	return rows, nil
}

func (im *Importer) Import(sn string, headerRowIndex int) error {
	var e error

	rows, e := im.Rows(sn, headerRowIndex)
	if e != nil {
		return fmt.Errorf("getting rows error: %s", e.Error())
	}

	if im.OnBeforeProcessing != nil {
		if e = im.OnBeforeProcessing(im, rows); e != nil {
			return fmt.Errorf("before processing error: %s", e.Error())
		}
	}

	if im.ImportData != nil {
		for _, r := range rows {
			if e = im.ImportData(im, r); e != nil {
				fmt.Printf("on import error: %s, data: %s\n", e.Error(), toolkit.JsonString(r))
			}
		}
	}

	if im.OnProcessed != nil {
		if e = im.OnProcessed(im); e != nil {
			return fmt.Errorf("after processing: %s", e.Error())
		}
	}

	return nil
}
