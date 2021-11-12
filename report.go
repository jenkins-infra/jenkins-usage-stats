package stats

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/beevik/etree"
)

var (
	// PieColors is the ordered list of strings to be used for coloring pie wedges.
	PieColors = []string{
		"BurlyWood",
		"CadetBlue",
		"red",
		"blue",
		"yellow",
		"green",
		"gold",
		"brown",
		"Azure",
		"pink",
		"khaki",
		"gray",
		"Aqua",
		"Aquamarine",
		"beige",
		"blueviolet",
		"Bisque",
		"coral",
		"darkblue",
		"crimson",
		"cyan",
		"darkred",
		"ivory",
		"lime",
		"maroon",
		"navy",
		"olive",
		"plum",
		"peru",
		"silver",
		"tan",
		"teal",
		"violet",
		/////
		"AliceBlue",
		"DarkOliveGreen",
		"Indigo",
		"MediumPurple",
		"Purple",
		"AntiqueWhite",
		"DarkOrange",
		"Ivory",
		"MediumSeaGreen",
		"Red",
		"Aqua",
		"DarkOrchid",
		"Khaki",
		"MediumSlateBlue",
		"RosyBrown",
		"AquaMarine",
		"DarkRed",
		"Lavender",
		"MediumSpringGreen",
		"RoyalBlue",
		"Azure",
		"DarkSalmon",
		"LavenderBlush",
		"MediumTurquoise",
		"SaddleBrown",
		"Beige",
		"DarkSeaGreen",
		"LawnGreen",
		"MediumVioletRed",
		"Salmon",
		"Bisque",
		"DarkSlateBlue",
		"LemonChiffon",
		"MidnightBlue",
		"SandyBrown",
		"Black",
		"DarkSlateGray",
		"LightBlue",
		"MintCream",
		"SeaGreen",
		"BlanchedAlmond",
		"DarkTurquoise",
		"LightCoral",
		"MistyRose",
		"SeaShell",
		"Blue",
		"DarkViolet",
		"LightCyan",
		"Moccasin",
		"Sienna",
		"BlueViolet",
		"DeepPink",
		"LightGoldenrodYellow",
		"NavajoWhite",
		"Silver",
		"Brown",
		"DeepSkyBlue",
		"LightGray",
		"Navy",
		"SkyBlue",
		"BurlyWood",
		"DimGray",
		"LightGreen",
		"OldLace",
		"SlateBlue",
		"CadetBlue",
		"DodgerBlue",
		"LightPink",
		"Olive",
		"SlateGray",
		"Chartreuse",
		"FireBrick",
		"LightSalmon",
		"OliveDrab",
		"Snow",
		"Chocolate",
		"FloralWhite",
		"LightSeaGreen",
		"Orange",
		"SpringGreen",
		"Coral",
		"ForestGreen",
		"LightSkyBlue",
		"OrangeRed",
		"SteelBlue",
		"CornFlowerBlue",
		"Fuchsia",
		"LightSlateGray",
		"Orchid",
		"Tan",
		"Cornsilk",
		"Gainsboro",
		"LightSteelBlue",
		"PaleGoldenRod",
		"Teal",
		"Crimson",
		"GhostWhite",
		"LightYellow",
		"PaleGreen",
		"Thistle",
		"Cyan",
		"Gold",
		"Lime",
		"PaleTurquoise",
		"Tomato",
		"DarkBlue",
		"GoldenRod",
		"LimeGreen",
		"PaleVioletRed",
		"Turquoise",
		"DarkCyan",
		"Gray",
		"Linen",
		"PapayaWhip",
		"Violet",
		"DarkGoldenRod",
		"Green",
		"Magenta",
		"PeachPuff",
		"Wheat",
		"DarkGray",
		"GreenYellow",
		"Maroon",
		"Peru",
		"White",
		"DarkGreen",
		"HoneyDew",
		"MediumAquaMarine",
		"Pink",
		"WhiteSmoke",
		"DarkKhaki",
		"HotPink",
		"MediumBlue",
		"Plum",
		"Yellow",
		"DarkMagenta",
		"IndianRed",
		"MediumOrchid",
		"PowderBlue",
		"YellowGreen",
	}
)

// PluginReport is written out as JSON for reports for each plugin
type PluginReport struct {
	Name               string             `json:"name"`
	Installations      map[string]uint64  `json:"installations"`
	MonthPercentages   map[string]float32 `json:"installationsPercentages"`
	PerVersion         map[string]uint64  `json:"installationsPerVersion"`
	VersionPercentages map[string]float32 `json:"installationsPercentagePerVersion"`
}

// JVMReport is marshalled to create jvms.json
type JVMReport struct {
	PerMonth   map[string]map[string]uint64 `json:"jvmStatsPerMonth"`
	PerMonth2x map[string]map[string]uint64 `json:"jvmStatsPerMonth_2.x"`
}

// InstallationReport is written out to generate installations.{json,csv}
type InstallationReport struct {
	Installations map[string]uint64 `json:"installations"`
}

// ToCSV returns a CSV representation of the InstallationReport
func (i InstallationReport) ToCSV() (string, error) {
	var records [][]string

	keys := make([]string, len(i.Installations))

	for k := range i.Installations {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		records = append(records, []string{k, fmt.Sprintf("%d", i.Installations[k])})
	}

	var builder strings.Builder
	writer := csv.NewWriter(&builder)
	if err := writer.WriteAll(records); err != nil {
		return "", err
	}

	return builder.String(), nil
}

// LatestPluginNumbersReport is written out to generate latestNumbers.{json,csv}
type LatestPluginNumbersReport struct {
	Month   int64             `json:"month"`
	Plugins map[string]uint64 `json:"plugins"`
}

// ToCSV returns a CSV representation of the LatestPluginNumbersReport
func (l LatestPluginNumbersReport) ToCSV() (string, error) {
	var records [][]string

	keys := make([]string, len(l.Plugins))

	for k := range l.Plugins {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		records = append(records, []string{k, fmt.Sprintf("%d", l.Plugins[k])})
	}

	var builder strings.Builder
	writer := csv.NewWriter(&builder)
	if err := writer.WriteAll(records); err != nil {
		return "", err
	}

	return builder.String(), nil
}

// CapabilitiesReport is written out to generate capabilities.{json,csv}
type CapabilitiesReport struct {
	Installations map[string]uint64 `json:"installations"`
}

// ToCSV returns a CSV representation of the CapabilitiesReport
func (i CapabilitiesReport) ToCSV() (string, error) {
	var records [][]string

	keys := make([]string, len(i.Installations))

	for k := range i.Installations {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		records = append(records, []string{k, fmt.Sprintf("%d", i.Installations[k])})
	}

	var builder strings.Builder
	writer := csv.NewWriter(&builder)
	if err := writer.WriteAll(records); err != nil {
		return "", err
	}

	return builder.String(), nil
}

type yearMonth struct {
	year  int
	month int
}

// GetInstallCountForVersions generates a map of Jenkins versions to install counts
// analogous to Groovy version's generateInstallationsJson
func GetInstallCountForVersions(db sq.BaseRunner, year, month int) (InstallationReport, error) {
	report := InstallationReport{Installations: map[string]uint64{}}
	rows, err := PSQL(db).Select("jenkins_versions.version as jvv", "count(*) as number").
		From(InstanceReportsTable).
		Join("jenkins_versions on instance_reports.version = jenkins_versions.id").
		Where(sq.Eq{"instance_reports.year": year}).
		Where(sq.Eq{"instance_reports.month": month}).
		Where(sq.GtOrEq{"instance_reports.count_for_month": 2}).
		Where(`jenkins_versions.version ~ '^\d' and jenkins_versions.version not like '%private%'`).
		GroupBy("jvv").
		Query()
	if err != nil {
		return report, err
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var v string
		var c uint64
		err := rows.Scan(&v, &c)
		if err != nil {
			return report, err
		}
		report.Installations[v] = c
	}

	return report, nil
}

// GetLatestPluginNumbers generates a map of plugin name and install counts
// analogous to Groovy version's generateLatestNumbersJson
func GetLatestPluginNumbers(db sq.BaseRunner, year, month int) (LatestPluginNumbersReport, error) {
	report := LatestPluginNumbersReport{
		Month:   startDateForYearMonth(year, month).Unix(),
		Plugins: map[string]uint64{},
	}
	rows, err := PSQL(db).Select("p.name as pn", "count(*) as number").
		From("instance_reports i, unnest(i.plugins) pr(id)").
		Join("plugins p on p.id = pr.id").
		Where(sq.Eq{"i.year": year}).
		Where(sq.Eq{"i.month": month}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		GroupBy("pn").
		Query()
	if err != nil {
		return report, err
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var p string
		var c uint64
		err := rows.Scan(&p, &c)
		if err != nil {
			return report, err
		}
		report.Plugins[p] = c
	}

	return report, nil
}

// GetCapabilities generates a map of Jenkins versions and install counts for that version and all earlier ones
// analogous to Groovy version's generateCapabilitiesJson
func GetCapabilities(db sq.BaseRunner, year, month int) (CapabilitiesReport, error) {
	report := CapabilitiesReport{Installations: map[string]uint64{}}
	rows, err := PSQL(db).Select("jenkins_versions.version as jvv", "count(*) as number").
		From(InstanceReportsTable).
		Join("jenkins_versions on instance_reports.version = jenkins_versions.id").
		Where(sq.Eq{"instance_reports.year": year}).
		Where(sq.Eq{"instance_reports.month": month}).
		Where(sq.GtOrEq{"instance_reports.count_for_month": 2}).
		Where(`jenkins_versions.version ~ '^\d' and jenkins_versions.version not like '%private%'`).
		GroupBy("jvv").
		OrderBy("jvv DESC").
		Query()
	if err != nil {
		return report, err
	}
	defer func() {
		_ = rows.Close()
	}()

	higherCapabilityCount := uint64(0)
	for rows.Next() {
		var p string
		var c uint64
		err := rows.Scan(&p, &c)
		if err != nil {
			return report, err
		}
		higherCapabilityCount += c
		report.Installations[p] = higherCapabilityCount
	}

	return report, nil
}

// GetJVMsReport returns the JVM install counts for all months
// analogous to Groovy version's generateJvmJson
func GetJVMsReport(db sq.BaseRunner) (JVMReport, error) {
	jvr := JVMReport{
		PerMonth:   map[string]map[string]uint64{},
		PerMonth2x: map[string]map[string]uint64{},
	}

	now := time.Now()

	months, err := allOrderedMonths(db, now.Year(), int(now.Month()))
	if err != nil {
		return jvr, err
	}
	jvmIDs, err := jvmIDsForJSON(db)
	if err != nil {
		return jvr, err
	}
	jenkinsIDs, err := jenkinsVersions2x(db)
	if err != nil {
		return jvr, err
	}

	baseStmt := PSQL(db).Select("jv.name as n", "count(*)").
		From("instance_reports i").
		Join("jvm_versions jv on jv.id = i.jvm_version_id").
		Where(sq.Eq{"jv.id": jvmIDs}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		GroupBy("n").
		OrderBy("n")

	for _, ym := range months {
		err = func() error {
			ts := startDateForYearMonth(ym.year, ym.month)
			tsStr := fmt.Sprintf("%d", ts.Unix())

			monthStmt := baseStmt.Where(sq.Eq{"i.year": ym.year}).Where(sq.Eq{"i.month": ym.month})
			rows, err := monthStmt.Query()
			if err != nil {
				return err
			}
			defer func() {
				_ = rows.Close()
			}()
			for rows.Next() {
				var name string
				var count uint64
				if _, ok := jvr.PerMonth[tsStr]; !ok {
					jvr.PerMonth[tsStr] = map[string]uint64{}
				}
				err = rows.Scan(&name, &count)
				if err != nil {
					return err
				}

				jvr.PerMonth[tsStr][name] = count
			}

			rows2x, err := monthStmt.Where(sq.Eq{"i.version": jenkinsIDs}).Query()
			if err != nil {
				return err
			}
			defer func() {
				_ = rows2x.Close()
			}()
			for rows2x.Next() {
				var name string
				var count uint64
				if _, ok := jvr.PerMonth2x[tsStr]; !ok {
					jvr.PerMonth2x[tsStr] = map[string]uint64{}
				}
				err = rows2x.Scan(&name, &count)
				if err != nil {
					return err
				}

				jvr.PerMonth2x[tsStr][name] = count
			}

			return nil
		}()
		if err != nil {
			return jvr, err
		}
	}

	return jvr, nil
}

// GetPluginReports generates reports for each plugin
// analogous to Groovy version's generatePluginsJson
func GetPluginReports(db sq.BaseRunner, currentYear, currentMonth int) ([]PluginReport, error) {
	previousMonth := startDateForYearMonth(currentYear, currentMonth).AddDate(0, -1, 0)
	prevMonthStr := fmt.Sprintf("%d", previousMonth.Unix())

	var reports []PluginReport

	pluginNames, err := allPluginNames(db)
	if err != nil {
		return nil, err
	}

	totalInstalls, err := installCountsByMonth(db, currentYear, currentMonth)
	if err != nil {
		return nil, err
	}

	for _, pn := range pluginNames {
		report := PluginReport{
			Name:               pn,
			Installations:      map[string]uint64{},
			MonthPercentages:   map[string]float32{},
			PerVersion:         map[string]uint64{},
			VersionPercentages: map[string]float32{},
		}

		installsByMonth, err := pluginInstallsByMonthForName(db, pn, currentYear, currentMonth)
		if err != nil {
			return nil, err
		}

		for monthStr, monthCount := range installsByMonth {
			report.Installations[monthStr] = monthCount
			report.MonthPercentages[monthStr] = float32(monthCount) * 100 / float32(totalInstalls[monthStr])
		}

		installsByVersion, err := pluginInstallsByVersionForName(db, pn, previousMonth.Year(), int(previousMonth.Month()))
		if err != nil {
			return nil, err
		}

		for versionStr, versionCount := range installsByVersion {
			report.PerVersion[versionStr] = versionCount
			report.VersionPercentages[versionStr] = float32(versionCount) * 100 / float32(totalInstalls[prevMonthStr])
		}

		reports = append(reports, report)
	}

	return reports, nil
}

// JenkinsVersionsForPluginVersions generates a report for each plugin's version, with a count of installs for each Jenkins version
// analogous to Groovy version's generateOldestJenkinsPerPlugin
func JenkinsVersionsForPluginVersions(db sq.BaseRunner, year, month int) (map[string]map[string]map[string]uint64, error) {
	maxVersionsForInstanceIDs, err := maxInstanceVersionForMonth(db, year, month)
	if err != nil {
		return nil, err
	}

	rows, err := PSQL(db).Select("p.name as pn", "p.version as pv", "i.instance_id as iid").
		From("instance_reports i, unnest(i.plugins) pr(id)").
		Join("plugins p on p.id = pr.id").
		Where(sq.Eq{"i.year": year}).
		Where(sq.Eq{"i.month": month}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		OrderBy("pn", "pv desc", "iid").
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	pluginMap := make(map[string]map[string]map[string]uint64)
	for rows.Next() {
		var pn, pv, iid string

		err := rows.Scan(&pn, &pv, &iid)
		if err != nil {
			return nil, err
		}

		// If we don't have a "valid" max Jenkins version for this instance, skip.
		if _, ok := maxVersionsForInstanceIDs[iid]; !ok {
			continue
		}

		if _, ok := pluginMap[pn]; !ok {
			pluginMap[pn] = make(map[string]map[string]uint64)
		}
		if _, ok := pluginMap[pn][pv]; !ok {
			pluginMap[pn][pv] = make(map[string]uint64)
		}
		if _, ok := pluginMap[pn][pv][iid]; !ok {
			pluginMap[pn][pv][iid] = 0
		}

		pluginMap[pn][pv][iid]++
	}

	return pluginMap, nil
}

// JobCountsForMonth gets the total number of each known job type in a month
// analogous to jobtype2Number in generateStats.groovy
func JobCountsForMonth(db sq.BaseRunner, year, month int) (map[string]uint64, error) {
	rows, err := PSQL(db).Select("j.name", "sum(jr.value::int) as total").
		From("instance_reports i, jsonb_each_text(i.jobs) jr").
		Join("job_types j on j.id = jr.key::int").
		Where(sq.Eq{"i.year": year}).
		Where(sq.Eq{"i.month": month}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		GroupBy("j.name").
		OrderBy("total asc").
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	jobMap := make(map[string]uint64)

	for rows.Next() {
		var name string
		var count uint64

		err = rows.Scan(&name, &count)
		if err != nil {
			return nil, err
		}
		jobMap[name] = count
	}

	return jobMap, nil
}

// OSCountsForMonth gets the total number of each known OS type in a month
// analogous to nodesOnOS2Number in generateStats.groovy
func OSCountsForMonth(db sq.BaseRunner, year, month int) (map[string]uint64, error) {
	rows, err := PSQL(db).Select("o.name", "sum(nr.value::int) as total").
		From("instance_reports i, jsonb_each_text(i.nodes) nr").
		Join("os_types o on o.id = nr.key::int").
		Where(sq.Eq{"i.year": year}).
		Where(sq.Eq{"i.month": month}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		GroupBy("o.name").
		OrderBy("total asc").
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	osMap := make(map[string]uint64)

	for rows.Next() {
		var name string
		var count uint64

		err = rows.Scan(&name, &count)
		if err != nil {
			return nil, err
		}
		osMap[name] = count
	}

	return osMap, nil
}

// CreateBarSVG takes a dataset and returns byte slices for the corresponding .svg and .csv files
func CreateBarSVG(title string, data map[string]uint64, scaleReduction int, sortByValue bool, filterFunc func(string, uint64) bool) ([]byte, []byte, error) {
	sortedData, maxVal := asSortedPairsAndMaxValue(data, sortByValue, filterFunc)

	viewWidth := (len(sortedData) * 15) + 50

	doc := etree.NewDocument()
	svg := doc.CreateElement("svg")
	_ = svg.CreateAttr("xmlns", "http://www.w3.org/2000/svg")
	_ = svg.CreateAttr("version", "1.1")
	_ = svg.CreateAttr("preserveAspectRatio", "xMidYMid meet")
	_ = svg.CreateAttr("viewBox", fmt.Sprintf("0 0 %d %f", viewWidth, (float32(maxVal)/float32(scaleReduction))+350))

	for idx, kv := range sortedData {
		barHeight := kv.value / uint64(scaleReduction)
		xAxis := (idx + 1) * 15
		yAxis := ((float32(maxVal) / float32(scaleReduction)) - float32(barHeight)) + 50
		textY := yAxis + float32(barHeight) + 5

		rect := svg.CreateElement("rect")
		_ = rect.CreateAttr("fill", "blue")
		_ = rect.CreateAttr("height", fmt.Sprintf("%d", barHeight))
		_ = rect.CreateAttr("stroke", "black")
		_ = rect.CreateAttr("width", "12")
		_ = rect.CreateAttr("x", fmt.Sprintf("%d", xAxis))
		_ = rect.CreateAttr("y", fmt.Sprintf("%f", yAxis))

		textElem := svg.CreateElement("text")
		_ = textElem.CreateAttr("x", fmt.Sprintf("%d", xAxis))
		_ = textElem.CreateAttr("y", fmt.Sprintf("%f", textY))
		_ = textElem.CreateAttr("font-family", "Tahoma")
		_ = textElem.CreateAttr("font-size", "12")
		_ = textElem.CreateAttr("transform", fmt.Sprintf("rotate(90 %d,%f)", xAxis, textY))
		_ = textElem.CreateAttr("text-rendering", "optimizeSpeed")
		_ = textElem.CreateAttr("fill", "#000000")
		textElem.SetText(fmt.Sprintf("%s (%d)", kv.key, kv.value))
	}

	titleElem := svg.CreateElement("text")
	_ = titleElem.CreateAttr("x", "10")
	_ = titleElem.CreateAttr("y", "40")
	_ = titleElem.CreateAttr("font-family", "Tahoma")
	_ = titleElem.CreateAttr("font-size", "20")
	_ = titleElem.CreateAttr("text-rendering", "optimizeSpeed")
	_ = titleElem.CreateAttr("fill", "#000000")
	titleElem.SetText(title)

	doc.Indent(2)
	body, err := doc.WriteToBytes()
	if err != nil {
		return nil, nil, err
	}

	var records [][]string
	for k, v := range data {
		records = append(records, []string{k, fmt.Sprintf("%d", v)})
	}

	var builder bytes.Buffer
	writer := csv.NewWriter(&builder)
	if err := writer.WriteAll(records); err != nil {
		return nil, nil, err
	}

	return body, builder.Bytes(), nil
}

// CreatePieSVG takes a dataset and returns byte slices for the corresponding .svg and .csv files
func CreatePieSVG(title string, data []uint64, centerX, centerY, radius, upperLeftX, upperLeftY int, labels []string, colors []string) ([]byte, []byte, error) {

	totalCount := uint64(0)
	for _, v := range data {
		totalCount += v
	}

	var angles []float64
	for i := range data {
		angles[i] = float64(data[i]) / float64(totalCount) * math.Pi * 2
	}

	startAngle := float64(0)
	squareHeight := 30

	viewWidth := upperLeftX + 350
	viewHeight := upperLeftY + (len(data) * squareHeight) + 30

	doc := etree.NewDocument()
	svg := doc.CreateElement("svg")
	_ = svg.CreateAttr("xmlns", "http://www.w3.org/2000/svg")
	_ = svg.CreateAttr("version", "1.1")
	_ = svg.CreateAttr("preserveAspectRatio", "xMidYMid meet")
	_ = svg.CreateAttr("viewBox", fmt.Sprintf("0 0 %d %d", viewWidth, viewHeight))

	titleElem := svg.CreateElement("text")
	_ = titleElem.CreateAttr("x", "30")
	_ = titleElem.CreateAttr("y", "40")
	_ = titleElem.CreateAttr("font-family", "sans-serif")
	_ = titleElem.CreateAttr("font-size", "16")
	titleElem.SetText(fmt.Sprintf("%s, total: %d", title, totalCount))

	for i, v := range data {
		endAngle := startAngle + angles[i]

		x1 := float64(centerX+radius) * math.Sin(startAngle)
		y1 := float64(centerY-radius) * math.Cos(startAngle)
		x2 := float64(centerX+radius) * math.Sin(endAngle)
		y2 := float64(centerY-radius) * math.Cos(endAngle)

		greaterThanHalfCircleAdjustment := 0
		if endAngle-startAngle > math.Pi {
			greaterThanHalfCircleAdjustment = 1
		}

		pathD := fmt.Sprintf("M %d,%d L %f,%f A %d,%d 0 %d 1 %f,%f Z", centerX, centerY, x1, y1, radius, radius, greaterThanHalfCircleAdjustment, x2, y2)

		pathElem := svg.CreateElement("path")
		_ = pathElem.CreateAttr("d", pathD)
		_ = pathElem.CreateAttr("fill", colors[i])
		_ = pathElem.CreateAttr("stroke", "black")
		_ = pathElem.CreateAttr("stroke-width", "1")

		// Next wedge starts at the end of this wedge
		startAngle = endAngle

		rectElem := svg.CreateElement("rect")
		_ = rectElem.CreateAttr("x", fmt.Sprintf("%d", upperLeftX))
		_ = rectElem.CreateAttr("y", fmt.Sprintf("%d", upperLeftY+squareHeight+i))
		_ = rectElem.CreateAttr("width", "20")
		_ = rectElem.CreateAttr("height", fmt.Sprintf("%d", squareHeight))
		_ = rectElem.CreateAttr("fill", colors[i])
		_ = rectElem.CreateAttr("stroke", "black")
		_ = rectElem.CreateAttr("stroke-width", "1")

		textElem := svg.CreateElement("text")
		_ = textElem.CreateAttr("x", fmt.Sprintf("%d", upperLeftX+30))
		_ = textElem.CreateAttr("y", fmt.Sprintf("%d", upperLeftY+squareHeight*i+18))
		_ = titleElem.CreateAttr("font-family", "sans-serif")
		_ = titleElem.CreateAttr("font-size", "16")
		titleElem.SetText(fmt.Sprintf("%s (%d)", labels[i], v))
	}

	doc.Indent(2)
	body, err := doc.WriteToBytes()
	if err != nil {
		return nil, nil, err
	}

	var records [][]string
	for i, v := range labels {
		records = append(records, []string{fmt.Sprintf("%d", data[i]), v})
	}

	var builder bytes.Buffer
	writer := csv.NewWriter(&builder)
	if err := writer.WriteAll(records); err != nil {
		return nil, nil, err
	}

	return body, builder.Bytes(), nil
}

// DefaultFilter returns true for all string/uint64 pairs
func DefaultFilter(_ string, _ uint64) bool {
	return true
}

type kvPair struct {
	key   string
	value uint64
}

func asSortedPairsAndMaxValue(data map[string]uint64, byValue bool, filterFunc func(string, uint64) bool) ([]kvPair, uint64) {
	maxVal := uint64(0)

	var sp []kvPair
	for k, v := range data {
		if filterFunc(k, v) {
			sp = append(sp, kvPair{
				key:   k,
				value: v,
			})
			if v > maxVal {
				maxVal = v
			}
		}
	}

	if byValue {
		sort.Slice(sp, func(i, j int) bool {
			return sp[i].value < sp[j].value
		})
	} else {
		sort.Slice(sp, func(i, j int) bool {
			return sp[i].key < sp[j].key
		})
	}

	return sp, maxVal
}

func pluginInstallsByMonthForName(db sq.BaseRunner, pluginName string, currentYear, currentMonth int) (map[string]uint64, error) {
	monthCount := make(map[string]uint64)

	rows, err := PSQL(db).Select("i.year", "i.month", "count(*)").
		From("instance_reports i, unnest(i.plugins) pr(id)").
		Join("plugins p on p.id = pr.id").
		Where(sq.Eq{"p.name": pluginName}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		Where(fmt.Sprintf("NOT (i.year = %d and i.month = %d)", currentYear, currentMonth)).
		OrderBy("i.year", "i.month").
		GroupBy("i.year", "i.month").
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var y, m int
		var c uint64

		err = rows.Scan(&y, &m, &c)
		if err != nil {
			return nil, err
		}

		monthTS := startDateForYearMonth(y, m)

		monthCount[fmt.Sprintf("%d", monthTS.Unix())] = c
	}

	return monthCount, nil
}

func pluginInstallsByVersionForName(db sq.BaseRunner, pluginName string, year, month int) (map[string]uint64, error) {
	monthCount := make(map[string]uint64)

	rows, err := PSQL(db).Select("p.version", "count(*)").
		From("instance_reports i, unnest(i.plugins) pr(id)").
		Join("plugins p on p.id = pr.id").
		Where(sq.Eq{"p.name": pluginName}).
		Where(sq.Eq{"i.year": year}).
		Where(sq.Eq{"i.month": month}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		OrderBy("p.version").
		GroupBy("p.version").
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var v string
		var c uint64

		err = rows.Scan(&v, &c)
		if err != nil {
			return nil, err
		}

		monthCount[v] = c
	}

	return monthCount, nil
}

// jvmIDsForJSON gets all jvm_versions IDs that we actually care about for reporting, filtering out eccentric versions.
func jvmIDsForJSON(db sq.BaseRunner) ([]uint64, error) {
	var jvmIDs []uint64
	rows, err := PSQL(db).Select("id").
		From(JVMVersionsTable).
		Where(`name ~ '^(\d\d|\d\.\d)$'`).
		Query()
	defer func() {
		_ = rows.Close()
	}()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id uint64
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		jvmIDs = append(jvmIDs, id)
	}

	return jvmIDs, nil
}

func jenkinsVersions2x(db sq.BaseRunner) ([]uint64, error) {
	var jvIDs []uint64
	rows, err := PSQL(db).Select("id").
		From(JenkinsVersionsTable).
		Where(`version ~ '^2\.'`).
		Query()
	defer func() {
		_ = rows.Close()
	}()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id uint64
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		jvIDs = append(jvIDs, id)
	}

	return jvIDs, nil
}

func allOrderedMonths(db sq.BaseRunner, currentYear, currentMonth int) ([]yearMonth, error) {
	var yearMonths []yearMonth
	rows, err := PSQL(db).Select("year", "month").
		From(InstanceReportsTable).
		Where("NOT (year = $1 and month = $2)", currentYear, currentMonth).
		OrderBy("year", "month").
		GroupBy("year", "month").
		Query()
	defer func() {
		_ = rows.Close()
	}()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		ym := yearMonth{}
		err = rows.Scan(&ym.year, &ym.month)
		if err != nil {
			return nil, err
		}
		yearMonths = append(yearMonths, ym)
	}

	return yearMonths, nil
}

func installCountsByMonth(db sq.BaseRunner, currentYear, currentMonth int) (map[string]uint64, error) {
	installs := make(map[string]uint64)

	rows, err := PSQL(db).Select("year", "month", "count(*)").
		From(InstanceReportsTable).
		Where("NOT (year = $1 and month = $2)", currentYear, currentMonth).
		GroupBy("year", "month").
		OrderBy("year", "month").
		Query()
	defer func() {
		_ = rows.Close()
	}()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var y, m int
		var c uint64

		err = rows.Scan(&y, &m, &c)
		if err != nil {
			return nil, err
		}

		startTS := fmt.Sprintf("%d", startDateForYearMonth(y, m).Unix())
		installs[startTS] = c
	}

	return installs, nil
}

func maxInstanceVersionForMonth(db sq.BaseRunner, year, month int) (map[string]string, error) {
	maxVersions := make(map[string]string)

	rows, err := PSQL(db).Select("i.instance_id", "max(jv.version)").
		From("instance_reports i").
		Join("jenkins_versions jv on jv.id = i.version").
		Where(sq.Eq{"i.year": year}).
		Where(sq.Eq{"i.month": month}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		Where(`jv.version not like '%private%'`).
		Where(`jv.version ~ '^\d'`).
		GroupBy("i.instance_id").
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var id, version string
		err = rows.Scan(&id, &version)
		if err != nil {
			return nil, err
		}
		maxVersions[id] = version
	}

	return maxVersions, nil
}

func allPluginNames(db sq.BaseRunner) ([]string, error) {
	var names []string

	rows, err := PSQL(db).Select("name").
		From(PluginsTable).
		GroupBy("name").
		Query()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var n string

		err = rows.Scan(&n)
		if err != nil {
			return nil, err
		}

		names = append(names, n)
	}

	return names, nil
}
