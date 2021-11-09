package pkg

import (
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
)

// PluginReportJSON is written out as JSON for reports for each plugin
type PluginReportJSON struct {
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

type yearMonth struct {
	year  int
	month int
}

// GetInstallCountForVersions generates a map of Jenkins versions to install counts
func GetInstallCountForVersions(db sq.BaseRunner, year, month string) (map[string]uint64, error) {
	rows, err := PSQL(db).Select("jenkins_versions.version as version", "count(*) as number").
		From(InstanceReportsTable).
		Join("jenkins_versions on instance_reports.version = jenkins_versions.id").
		Where(sq.Eq{"instance_reports.year": year}).
		Where(sq.Eq{"instance_reports.month": month}).
		Where(sq.GtOrEq{"instance_reports.count_for_month": 2}).
		Where(`jenkins_versions.version ~ '^\d' and jenkins_versions.version not like '%private%'`).
		GroupBy("version").
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	versionMap := make(map[string]uint64)
	for rows.Next() {
		var v string
		var c uint64
		err := rows.Scan(&v, &c)
		if err != nil {
			return nil, err
		}
		versionMap[v] = c
	}

	return versionMap, nil
}

// GetPluginCounts generates a map of plugin name and install counts
func GetPluginCounts(db sq.BaseRunner, year, month int) (map[string]uint64, error) {
	rows, err := PSQL(db).Select("p.name as pn", "count(*) as number").
		From("instance_reports i").
		From("unnest(i.plugins) pr(id)").
		Join("plugins p on p.id = pr.id").
		Where(sq.Eq{"i.year": year}).
		Where(sq.Eq{"i.month": month}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		GroupBy("pn").
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	pluginMap := make(map[string]uint64)
	for rows.Next() {
		var p string
		var c uint64
		err := rows.Scan(&p, &c)
		if err != nil {
			return nil, err
		}
		pluginMap[p] = c
	}

	return pluginMap, nil
}

// GetCapabilities generates a map of Jenkins versions and install counts for that version and all earlier ones
func GetCapabilities(db sq.BaseRunner, year, month string) (map[string]uint64, error) {
	rows, err := PSQL(db).Select("jenkins_versions.version as version", "count(*) as number").
		From(InstanceReportsTable).
		Join("jenkins_versions on instance_reports.version = jenkins_versions.id").
		Where(sq.Eq{"instance_reports.year": year}).
		Where(sq.Eq{"instance_reports.month": month}).
		Where(sq.GtOrEq{"instance_reports.count_for_month": 2}).
		Where(`jenkins_versions.version ~ '^\d' and jenkins_versions.version not like '%private%'`).
		GroupBy("version").
		OrderBy("version DESC").
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	higherCapabilityCount := uint64(0)
	versionMap := make(map[string]uint64)
	for rows.Next() {
		var p string
		var c uint64
		err := rows.Scan(&p, &c)
		if err != nil {
			return nil, err
		}
		higherCapabilityCount += c
		versionMap[p] = higherCapabilityCount
	}

	return versionMap, nil
}

// GetJVMsReport returns the JVM install counts for all months
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

	baseStmt := PSQL(db).Select("jv.name as name", "count(*)").
		From(InstanceReportsTable).
		Join(fmt.Sprintf("%s as jv on jv.id = %s.jvm_version_id", JVMVersionsTable, InstanceReportsTable)).
		Where(sq.Eq{"jv.name": jvmIDs}).
		GroupBy("name").
		OrderBy("name")

	for _, ym := range months {
		err = func() error {
			ts := startDateForYearMonth(ym.year, ym.month)
			tsStr := fmt.Sprintf("%d", ts.Unix())

			monthStmt := baseStmt.Where(sq.Eq{"instance_reports.year": ym.year}).Where(sq.Eq{"instance_reports.month": ym.month})
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

			rows2x, err := monthStmt.Where(sq.Eq{"version": jenkinsIDs}).Query()
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
func GetPluginReports(db sq.BaseRunner, currentYear, currentMonth int) ([]PluginReportJSON, error) {
	previousMonth := startDateForYearMonth(currentYear, currentMonth).AddDate(0, -1, 0)
	prevMonthStr := fmt.Sprintf("%d", previousMonth.Unix())

	var reports []PluginReportJSON

	pluginNames, err := allPluginNames(db)
	if err != nil {
		return nil, err
	}

	totalInstalls, err := installCountsByMonth(db, currentYear, currentMonth)
	if err != nil {
		return nil, err
	}

	for _, pn := range pluginNames {
		report := PluginReportJSON{
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

func pluginInstallsByMonthForName(db sq.BaseRunner, pluginName string, currentYear, currentMonth int) (map[string]uint64, error) {
	monthCount := make(map[string]uint64)

	rows, err := PSQL(db).Select("i.year", "i.month", "count(*)").
		From("instance_reports i").
		From("unnest(i.plugins) pr(id)").
		Join("plugins p on p.id = pr.id").
		Where(sq.Eq{"p.name": pluginName}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		Where("NOT (i.year = $1 and i.month = $2)", currentYear, currentMonth).
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
		From("instance_reports i").
		From("unnest(i.plugins) pr(id)").
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

	rows, err := PSQL(db).Select("instance_reports.instance_id", "max(jenkins_versions.version)").
		From("instance_reports").
		Join("jenkins_versions on jenkins_versions.id = instance_reports.version").
		Where(sq.Eq{"instance_reports.year": year}).
		Where(sq.Eq{"instance_reports.month": month}).
		Where(sq.GtOrEq{"instance_reports.count_for_month": 2}).
		Where(`jenkins_versions.version ~ '^\d' and jenkins_versions.version not like '%private%'`).
		GroupBy("instance_reports.instance_id").
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

func pluginVersionsForInstances(db sq.BaseRunner, year, month int) (map[string]map[string]map[string]uint64, error) {
	maxVersionsForInstanceIDs, err := maxInstanceVersionForMonth(db, year, month)
	if err != nil {
		return nil, err
	}

	rows, err := PSQL(db).Select("p.name as pn", "p.version as pv", "i.instance_id as iid").
		From("instance_reports i").
		From("unnest(i.plugins) pr(id)").
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
