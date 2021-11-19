package stats

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
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

	//go:embed templates/versionDistroPlugin.html.tmpl
	// VersionDistributionTemplate is the Golang template used for generating plugin version distribution HTML.
	VersionDistributionTemplate string

	//go:embed templates/versionDistroIndex.html.tmpl
	// VersionDistributionIndexTemplate is the Golang template used for generating the plugin version distribution index.
	VersionDistributionIndexTemplate string

	//go:embed templates/svgs.html.tmpl
	// SVGsIndexTemplate is the Golang template used for generating the svg directory HTML index.
	SVGsIndexTemplate string

	//go:embed templates/pitIndex.html.tmpl
	// PITIndexTemplate is the Golang template used for generating the plugin-installation-trend HTML index.
	PITIndexTemplate string
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
	var keys []string

	for k := range i.Installations {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var builder strings.Builder

	for _, k := range keys {
		_, err := builder.Write([]byte(fmt.Sprintf(`"%s","%d"`+"\n", k, i.Installations[k])))
		if err != nil {
			return "", err
		}
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
	var keys []string

	for k := range l.Plugins {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var builder strings.Builder

	for _, k := range keys {
		_, err := builder.Write([]byte(fmt.Sprintf(`"%s","%d"`+"\n", k, l.Plugins[k])))
		if err != nil {
			return "", err
		}
	}

	return builder.String(), nil
}

// CapabilitiesReport is written out to generate capabilities.{json,csv}
type CapabilitiesReport struct {
	Installations map[string]uint64 `json:"installations"`
}

// ToCSV returns a CSV representation of the CapabilitiesReport
func (i CapabilitiesReport) ToCSV() (string, error) {
	var keys []string

	for k := range i.Installations {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var builder strings.Builder

	for _, k := range keys {
		_, err := builder.Write([]byte(fmt.Sprintf(`"%s","%d"`+"\n", k, i.Installations[k])))
		if err != nil {
			return "", err
		}
	}

	return builder.String(), nil
}

type yearMonth struct {
	year  int
	month int
}

type monthForHTML struct {
	Year  int
	Num   string
	Name  string
	AsStr string
}

// GenerateReport creates the JSON, CSV, SVG, and HTML files for a monthly report
// TODO: Data sets don't seem to be complete compared to original, but that could just be a filtering thing? I've only compared 2009/2010 data so far.
func GenerateReport(db sq.BaseRunner, currentYear, currentMonth int, baseDir string) error {
	err := os.MkdirAll(baseDir, 0755) //nolint:gosec
	if err != nil {
		return err
	}

	pitDir := filepath.Join(baseDir, "plugin-installation-trend")
	err = os.MkdirAll(pitDir, 0755) //nolint:gosec
	if err != nil {
		return err
	}

	svgDir := filepath.Join(baseDir, "jenkins-stats/svg")
	err = os.MkdirAll(svgDir, 0755) //nolint:gosec
	if err != nil {
		return err
	}

	pvDir := filepath.Join(baseDir, "pluginversions")
	err = os.MkdirAll(pvDir, 0755) //nolint:gosec
	if err != nil {
		return err
	}

	previousMonth := startDateForYearMonth(currentYear, currentMonth).AddDate(0, -1, 0)
	prevYear := previousMonth.Year()
	prevMonth := int(previousMonth.Month())

	icStart := time.Now()
	installCount, err := GetInstallCountForVersions(db, prevYear, prevMonth)
	if err != nil {
		return err
	}
	icAsJSON, err := json.MarshalIndent(installCount, "", "  ")
	if err != nil {
		return err
	}

	err = writeFile(filepath.Join(pitDir, "installations.json"), icAsJSON)
	if err != nil {
		return err
	}
	icAsCSV, err := installCount.ToCSV()
	if err != nil {
		return err
	}
	err = writeFile(filepath.Join(pitDir, "installations.csv"), []byte(icAsCSV))
	if err != nil {
		return err
	}
	fmt.Printf("installCount time: %s\n", time.Since(icStart))

	vdStart := time.Now()
	jvpv, err := GenerateVersionDistributions(db, prevYear, prevMonth, pvDir)
	if err != nil {
		return err
	}
	fmt.Printf("versionDistribution time: %s\n", time.Since(vdStart))

	prStart := time.Now()
	// GetPluginReports expects to get the _current_ year/month so it can exclude that from its reports.
	pluginReports, err := GetPluginReports(db, currentYear, currentMonth)
	if err != nil {
		return err
	}
	for _, pr := range pluginReports {
		prAsJSON, err := json.MarshalIndent(pr, "", "  ")
		if err != nil {
			return err
		}
		err = writeFile(filepath.Join(pitDir, fmt.Sprintf("%s.stats.json", pr.Name)), prAsJSON)
		if err != nil {
			return err
		}
	}
	fmt.Printf("pluginReport time: %s\n", time.Since(prStart))

	lnStart := time.Now()
	latestNumbers, err := GetLatestPluginNumbers(db, prevYear, prevMonth)
	if err != nil {
		return err
	}
	lnAsJSON, err := json.MarshalIndent(latestNumbers, "", "  ")
	if err != nil {
		return err
	}
	lnAsCSV, err := latestNumbers.ToCSV()
	if err != nil {
		return err
	}
	err = writeFile(filepath.Join(pitDir, "latestNumbers.json"), lnAsJSON)
	if err != nil {
		return err
	}
	err = writeFile(filepath.Join(pitDir, "latestNumbers.csv"), []byte(lnAsCSV))
	if err != nil {
		return err
	}
	fmt.Printf("latestNumbers time: %s\n", time.Since(lnStart))

	capStart := time.Now()
	capabilities, err := GetCapabilities(db, prevYear, prevMonth)
	if err != nil {
		return err
	}
	capAsJSON, err := json.MarshalIndent(capabilities, "", "  ")
	if err != nil {
		return err
	}
	capAsCSV, err := capabilities.ToCSV()
	if err != nil {
		return err
	}
	err = writeFile(filepath.Join(pitDir, "capabilities.json"), capAsJSON)
	if err != nil {
		return err
	}
	err = writeFile(filepath.Join(pitDir, "capabilities.csv"), []byte(capAsCSV))
	if err != nil {
		return err
	}
	fmt.Printf("capabilities time: %s\n", time.Since(capStart))

	jvmStart := time.Now()
	// GetJVMsReport expects to get the _current_ year/month so that month can be excluded.
	jvms, err := GetJVMsReport(db, currentYear, currentMonth)
	if err != nil {
		return err
	}
	jvmsAsJSON, err := json.MarshalIndent(jvms, "", "  ")
	if err != nil {
		return err
	}
	err = writeFile(filepath.Join(pitDir, "jvms.json"), jvmsAsJSON)
	if err != nil {
		return err
	}
	fmt.Printf("jvms time: %s\n", time.Since(jvmStart))

	allMonths, err := allOrderedMonths(db, currentYear, currentMonth)
	if err != nil {
		return err
	}

	var monthsForHTML []monthForHTML

	installCountByMonth := make(map[string]uint64)
	jobCountByMonth := make(map[string]uint64)
	nodeCountByMonth := make(map[string]uint64)
	pluginCountByMonth := make(map[string]uint64)

	svgStart := time.Now()
	for _, ym := range allMonths {
		monthStr := fmt.Sprintf("%d%02d", ym.year, ym.month)
		monthsForHTML = append(monthsForHTML, monthForHTML{
			Year:  ym.year,
			Num:   fmt.Sprintf("%02d", ym.month),
			Name:  time.Month(ym.month).String(),
			AsStr: monthStr,
		})

		installCountByMonth[monthStr] = 0
		jobCountByMonth[monthStr] = 0
		nodeCountByMonth[monthStr] = 0
		pluginCountByMonth[monthStr] = 0

		ir, err := GetInstallCountForVersions(db, ym.year, ym.month)
		if err != nil {
			return err
		}

		for _, c := range ir.Installations {
			installCountByMonth[monthStr] += c
		}

		irSVG, irCSV, err := CreateBarSVG(fmt.Sprintf("Jenkins installations (total: %d)", installCountByMonth[monthStr]), ir.Installations, 10, false, DefaultFilter)
		if err != nil {
			return err
		}

		if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-jenkins.svg", monthStr)), irSVG); err != nil {
			return err
		}
		if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-jenkins.csv", monthStr)), irCSV); err != nil {
			return err
		}

		pr, err := GetLatestPluginNumbers(db, ym.year, ym.month)
		if err != nil {
			return err
		}
		for _, c := range pr.Plugins {
			pluginCountByMonth[monthStr] += c
		}

		prSVG, prCSV, err := CreateBarSVG(fmt.Sprintf("Plugin installations (total: %d)", pluginCountByMonth[monthStr]), pr.Plugins, 100, true, DefaultFilter)
		if err != nil {
			return err
		}
		if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-plugins.svg", monthStr)), prSVG); err != nil {
			return err
		}
		if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-plugins.csv", monthStr)), prCSV); err != nil {
			return err
		}

		for _, topNum := range []uint64{500, 100, 2500} {
			topPRSVG, topPRCSV, err := CreateBarSVG(fmt.Sprintf("Plugin installations (installations > %d)", topNum), pr.Plugins, 100, true, func(s string, u uint64) bool {
				return u > topNum
			})
			if err != nil {
				return err
			}
			if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-top-plugins%d.svg", monthStr, topNum)), topPRSVG); err != nil {
				return err
			}
			if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-top-plugins%d.csv", monthStr, topNum)), topPRCSV); err != nil {
				return err
			}
		}

		osR, err := OSCountsForMonth(db, ym.year, ym.month)
		if err != nil {
			return err
		}

		var osNames []string
		var osNumbers []uint64

		for n := range osR {
			osNames = append(osNames, n)
		}

		sort.Strings(osNames)

		for _, n := range osNames {
			nodeCountByMonth[monthStr] += osR[n]
			osNumbers = append(osNumbers, osR[n])
		}

		osBarSVG, osBarCSV, err := CreateBarSVG(fmt.Sprintf("Nodes (total: %d)", nodeCountByMonth[monthStr]), osR, 10, true, DefaultFilter)
		if err != nil {
			return err
		}
		if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-nodes.svg", monthStr)), osBarSVG); err != nil {
			return err
		}
		if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-nodes.csv", monthStr)), osBarCSV); err != nil {
			return err
		}

		osPieSVG, osPieCSV, err := CreatePieSVG("Nodes", osNumbers, 200, 300, 150, 370, 20, osNames, PieColors)
		if err != nil {
			return err
		}
		if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-nodesPie.svg", monthStr)), osPieSVG); err != nil {
			return err
		}
		if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-nodesPie.csv", monthStr)), osPieCSV); err != nil {
			return err
		}

		jr, err := JobCountsForMonth(db, ym.year, ym.month)
		if err != nil {
			return err
		}

		for _, c := range jr {
			jobCountByMonth[monthStr] += c
		}

		jobsSVG, jobsCSV, err := CreateBarSVG(fmt.Sprintf("Jobs (total: %d)", jobCountByMonth[monthStr]), jr, 1000, true, DefaultFilter)
		if err != nil {
			return err
		}
		if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-jobs.svg", monthStr)), jobsSVG); err != nil {
			return err
		}
		if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-jobs.csv", monthStr)), jobsCSV); err != nil {
			return err
		}

		execR, err := ExecutorCountsForMonth(db, ym.year, ym.month)
		if err != nil {
			return err
		}

		totalExecs := uint64(0)
		for _, c := range execR {
			totalExecs += c
		}

		execSVG, execCSV, err := CreateBarSVG(fmt.Sprintf("Executors per install (total: %d)", totalExecs), execR, 25, false, DefaultFilter)
		if err != nil {
			return err
		}
		if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-total-executors.svg", monthStr)), execSVG); err != nil {
			return err
		}
		if err := writeFile(filepath.Join(svgDir, fmt.Sprintf("%s-total-executors.csv", monthStr)), execCSV); err != nil {
			return err
		}
	}

	totalJenkinsSVG, totalJenkinsCSV, err := CreateBarSVG("Total Jenkins installations", installCountByMonth, 100, false, DefaultFilter)
	if err != nil {
		return err
	}
	if err := writeFile(filepath.Join(svgDir, "total-jenkins.svg"), totalJenkinsSVG); err != nil {
		return err
	}
	if err := writeFile(filepath.Join(svgDir, "total-jenkins.csv"), totalJenkinsCSV); err != nil {
		return err
	}

	totalJobsSVG, totalJobsCSV, err := CreateBarSVG("Total jobs", jobCountByMonth, 1000, false, DefaultFilter)
	if err != nil {
		return err
	}
	if err := writeFile(filepath.Join(svgDir, "total-jobs.svg"), totalJobsSVG); err != nil {
		return err
	}
	if err := writeFile(filepath.Join(svgDir, "total-jobs.csv"), totalJobsCSV); err != nil {
		return err
	}

	totalNodesSVG, totalNodesCSV, err := CreateBarSVG("Total nodes", nodeCountByMonth, 100, false, DefaultFilter)
	if err != nil {
		return err
	}
	if err := writeFile(filepath.Join(svgDir, "total-nodes.svg"), totalNodesSVG); err != nil {
		return err
	}
	if err := writeFile(filepath.Join(svgDir, "total-nodes.csv"), totalNodesCSV); err != nil {
		return err
	}

	totalPluginsSVG, totalPluginsCSV, err := CreateBarSVG("Total Plugin installations", pluginCountByMonth, 1000, false, DefaultFilter)
	if err != nil {
		return err
	}
	if err := writeFile(filepath.Join(svgDir, "total-plugins.svg"), totalPluginsSVG); err != nil {
		return err
	}
	if err := writeFile(filepath.Join(svgDir, "total-plugins.csv"), totalPluginsCSV); err != nil {
		return err
	}

	totalFiles := []string{"total-plugins", "total-jobs", "total-jenkins", "total-nodes"}

	idxTmpl, err := template.New("svgs-index").Parse(SVGsIndexTemplate)
	if err != nil {
		return err
	}

	idxFile, err := os.OpenFile(filepath.Join(svgDir, "svgs.html"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644) //nolint:gosec
	if err != nil {
		return err
	}
	defer func() {
		_ = idxFile.Close()
	}()

	err = idxTmpl.Execute(idxFile, map[string]interface{}{
		"totalFiles": totalFiles,
		"months":     monthsForHTML,
	})
	if err != nil {
		return err
	}
	fmt.Printf("svgs time: %s\n", time.Since(svgStart))

	jvpvJSON, err := json.MarshalIndent(jvpv, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFile(filepath.Join(pitDir, "jenkins-versions-per-plugin-version.json"), jvpvJSON); err != nil {
		return err
	}

	var pluginNames []string
	for pn := range latestNumbers.Plugins {
		pluginNames = append(pluginNames, pn)
	}
	sort.Strings(pluginNames)

	pitTmpl, err := template.New("pit-index").Parse(PITIndexTemplate)
	if err != nil {
		return err
	}

	pitFile, err := os.OpenFile(filepath.Join(pitDir, "index.html"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644) //nolint:gosec
	if err != nil {
		return err
	}
	defer func() {
		_ = pitFile.Close()
	}()

	return pitTmpl.Execute(pitFile, map[string]interface{}{
		"jsonFiles":   []string{"installations", "latestNumbers", "capabilities", "jenkins-version-per-plugin-version", "jvms"},
		"pluginNames": pluginNames,
	})
}

// GetInstallCountForVersions generates a map of Jenkins versions to install counts
// analogous to Groovy version's generateInstallationsJson
func GetInstallCountForVersions(db sq.BaseRunner, year, month int) (InstallationReport, error) {
	report := InstallationReport{Installations: map[string]uint64{}}
	rows, err := PSQL(db).Select("jv.version as jvv", "count(*) as number").
		From("instance_reports i").
		Join("jenkins_versions jv on i.version = jv.id").
		Where(sq.Eq{"i.year": year}).
		Where(sq.Eq{"i.month": month}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		Where("jv.version ~ '^\\d'").
		GroupBy("jvv").
		OrderBy("jvv").
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
	rows, err := PSQL(db).Select("jv.version as jvv", "count(*) as number").
		From("instance_reports i").
		Join("jenkins_versions jv on i.version = jv.id").
		Where(sq.Eq{"i.year": year}).
		Where(sq.Eq{"i.month": month}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		Where("jv.version ~ '^\\d'").
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
func GetJVMsReport(db sq.BaseRunner, year, month int) (JVMReport, error) {
	jvr := JVMReport{
		PerMonth:   map[string]map[string]uint64{},
		PerMonth2x: map[string]map[string]uint64{},
	}

	months, err := allOrderedMonths(db, year, month)
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

	idsToName, err := pluginIDsToPlugin(db)
	if err != nil {
		return nil, err
	}

	totalInstalls, err := installCountsByMonth(db, currentYear, currentMonth)
	if err != nil {
		return nil, err
	}

	installsByMonth, err := pluginInstallsByMonthForName(db, currentYear, currentMonth, idsToName)
	if err != nil {
		return nil, err
	}

	installsByVersion, err := pluginInstallsByVersionForName(db, previousMonth.Year(), int(previousMonth.Month()), idsToName)
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

		for versionStr, versionCount := range installsByVersion[pn] {
			report.PerVersion[versionStr] = versionCount
			report.VersionPercentages[versionStr] = float32(versionCount) * 100 / float32(totalInstalls[prevMonthStr])
		}

		for monthStr, monthCount := range installsByMonth[pn] {
			report.Installations[monthStr] = monthCount
			report.MonthPercentages[monthStr] = float32(monthCount) * 100 / float32(totalInstalls[monthStr])
		}

		reports = append(reports, report)
	}

	return reports, nil
}

// GenerateVersionDistributions writes out HTML files for each plugin's version distribution
func GenerateVersionDistributions(db sq.BaseRunner, year, month int, outputDir string) (map[string]map[string]map[string]uint64, error) {
	jvpv, err := JenkinsVersionsForPluginVersions(db, year, month)
	if err != nil {
		return nil, err
	}

	tmpl, err := template.New("versionDistribution").Parse(VersionDistributionTemplate)
	if err != nil {
		return nil, err
	}

	var pluginNames []string

	for k, v := range jvpv {
		versionInfo, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		outFile, err := os.OpenFile(filepath.Join(outputDir, fmt.Sprintf("%s.html", k)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644) //nolint:gosec
		if err != nil {
			return nil, err
		}
		err = tmpl.Execute(outFile, map[string]interface{}{
			"pluginName":        k,
			"pluginVersionData": string(versionInfo),
		})
		if err != nil {
			return nil, err
		}
		err = outFile.Close()
		if err != nil {
			return nil, err
		}

		pluginNames = append(pluginNames, k)
	}

	sort.Strings(pluginNames)

	indexTmpl, err := template.New("versionDistributionIndex").Parse(VersionDistributionIndexTemplate)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(filepath.Join(outputDir, "index.html"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644) //nolint:gosec
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = indexFile.Close()
	}()

	return jvpv, indexTmpl.Execute(indexFile, map[string]interface{}{"pluginNames": pluginNames})
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

// ExecutorCountsForMonth gets the total number of executors for each Jenkins version in a month
// analogous to executorCount2Number in generateStats.groovy
func ExecutorCountsForMonth(db sq.BaseRunner, year, month int) (map[string]uint64, error) {
	rows, err := PSQL(db).Select("jv.version", "sum(i.executors) as total").
		From("instance_reports i").
		Join("jenkins_versions jv on jv.id = i.version").
		Where(sq.Eq{"i.year": year}).
		Where(sq.Eq{"i.month": month}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		Where("jv.version ~ '^\\d'").
		GroupBy("jv.version").
		OrderBy("jv.version").
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	versionMap := make(map[string]uint64)

	for rows.Next() {
		var name string
		var count uint64

		err = rows.Scan(&name, &count)
		if err != nil {
			return nil, err
		}
		versionMap[name] = count
	}

	return versionMap, nil
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
// TODO: Something's awry in ordering here.
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

	var builder bytes.Buffer
	for _, v := range sortedData {
		_, err = builder.Write([]byte(fmt.Sprintf(`"%s","%d"`+"\n", v.key, v.value)))
		if err != nil {
			return nil, nil, err
		}
	}

	return body, builder.Bytes(), nil
}

// CreatePieSVG takes a dataset and returns byte slices for the corresponding .svg and .csv files
func CreatePieSVG(title string, data []uint64, centerX, centerY, radius, upperLeftX, upperLeftY int, labels []string, colors []string) ([]byte, []byte, error) {

	totalCount := uint64(0)
	for _, v := range data {
		totalCount += v
	}

	angles := make([]float64, len(data))
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

	var builder bytes.Buffer
	for i, v := range labels {
		_, err = builder.Write([]byte(fmt.Sprintf(`"%d","%s"`+"\n", data[i], v)))
		if err != nil {
			return nil, nil, err
		}
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

func pluginInstallsByMonthForName(db sq.BaseRunner, currentYear, currentMonth int, idToPlugin map[uint64]Plugin) (map[string]map[string]uint64, error) {
	monthCount := make(map[string]map[string]uint64)

	rows, err := PSQL(db).Select("pr.id", "i.year", "i.month", "count(*)").
		From("instance_reports i, unnest(i.plugins) pr(id)").
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		Where(fmt.Sprintf("NOT (i.year = %d and i.month = %d)", currentYear, currentMonth)).
		OrderBy("pr.id", "i.year", "i.month").
		GroupBy("pr.id", "i.year", "i.month").
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var i uint64
		var y, m int
		var c uint64

		err = rows.Scan(&i, &y, &m, &c)
		if err != nil {
			return nil, err
		}

		p, ok := idToPlugin[i]
		if !ok {
			return nil, fmt.Errorf("no plugin found for id %d", i)
		}
		monthTS := startDateForYearMonth(y, m)
		if _, ok := monthCount[p.Name]; !ok {
			monthCount[p.Name] = make(map[string]uint64)
		}
		mStr := fmt.Sprintf("%d", monthTS.Unix())
		if _, ok := monthCount[p.Name][mStr]; !ok {
			monthCount[p.Name][mStr] = 0
		}
		monthCount[p.Name][mStr] += c
	}

	return monthCount, nil
}

func pluginInstallsByVersionForName(db sq.BaseRunner, year, month int, idToPlugin map[uint64]Plugin) (map[string]map[string]uint64, error) {
	monthCount := make(map[string]map[string]uint64)

	rows, err := PSQL(db).Select("pr.id", "count(*)").
		From("instance_reports i, unnest(i.plugins) pr(id)").
		Where(sq.Eq{"i.year": year}).
		Where(sq.Eq{"i.month": month}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		OrderBy("pr.id").
		GroupBy("pr.id").
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var i uint64
		var c uint64

		err = rows.Scan(&i, &c)
		if err != nil {
			return nil, err
		}

		p, ok := idToPlugin[i]
		if !ok {
			return nil, fmt.Errorf("no plugin found for id %d", i)
		}

		if _, ok := monthCount[p.Name]; !ok {
			monthCount[p.Name] = make(map[string]uint64)
		}
		monthCount[p.Name][p.Version] = c
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
		Where(fmt.Sprintf("NOT (year = %d and month = %d)", currentYear, currentMonth)).
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
		Where(fmt.Sprintf("NOT (year = %d and month = %d)", currentYear, currentMonth)).
		Where(sq.GtOrEq{"count_for_month": 2}).
		GroupBy("year", "month").
		OrderBy("year", "month").
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

func pluginIDsToPlugin(db sq.BaseRunner) (map[uint64]Plugin, error) {
	plugins := make(map[uint64]Plugin)

	rows, err := PSQL(db).Select("id", "name", "version").
		From(PluginsTable).
		Query()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var i uint64
		var n, v string

		err = rows.Scan(&i, &n, &v)
		if err != nil {
			return nil, err
		}

		plugins[i] = Plugin{
			ID:      i,
			Name:    n,
			Version: v,
		}
	}

	return plugins, nil
}

func writeFile(filename string, data []byte) error {
	return ioutil.WriteFile(filename, data, 0644) //nolint:gosec
}
