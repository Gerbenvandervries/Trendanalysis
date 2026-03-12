#!/bin/bash

#
##
### Environment and Bash sanity.
##
#
if [[ "${BASH_VERSINFO[0]}" -lt 4 ]]
then
	echo "Sorry, you need at least bash 4.x to use ${0}." >&2
	exit 1
fi

set -e # Exit if any subcommand or pipeline returns a non-zero exit status.
set -u # Raise exception if variable is unbound. Combined with set -e will halt execution when an unbound variable is encountered.
set -o pipefail # Fail when any command in series of piped commands failed as opposed to only when the last command failed.

umask 0027

# Env vars.
export TMPDIR="${TMPDIR:-/tmp}" # Default to /tmp if $TMPDIR was not defined.
SCRIPT_NAME="$(basename "${0}")"
SCRIPT_NAME="${SCRIPT_NAME%.*sh}"
INSTALLATION_DIR="$(cd -P "$(dirname "${0}")/.." && pwd)"
LIB_DIR="${INSTALLATION_DIR}/lib"
CFG_DIR="${INSTALLATION_DIR}/etc"
HOSTNAME_SHORT="$(hostname -s)"
ROLE_USER="$(whoami)"
REAL_USER="$(logname 2>/dev/null || echo 'no login name')"


#
##
### General functions.
##
#
if [[ -f "${LIB_DIR}/sharedFunctions.bash" && -r "${LIB_DIR}/sharedFunctions.bash" ]]
then
	# shellcheck source=lib/sharedFunctions.bash
	source "${LIB_DIR}/sharedFunctions.bash"
else
	printf '%s\n' "FATAL: cannot find or cannot access sharedFunctions.bash"
	trap - EXIT
	exit 1
fi

function showHelp() {
		#
		# Display commandline help on STDOUT.
		#
		cat <<EOH
===============================================================================================================
Script to collect QC data from multiple sources and stores it in a ChronQC datatbase. This database is used to generate ChronQC reports.

Usage:

		$(basename "${0}") OPTIONS

Options:

		-h   Show this help.
		-g   Group.
		-d InputDataType dragen|projects|RNAprojects|ogm|darwin|openarray|rawdata 
		Providing InputDataType (or list: dragen,projects) to run only a specific data type for testing or debugging.
		-l   Log level.
		Must be one of TRACE, DEBUG, INFO (default), WARN, ERROR or FATAL.

Config and dependencies:

		This script needs 3 config files, which must be located in ${CFG_DIR}:
		1. <group>.cfg		for the group specified with -g
		2. <host>.cfg		for this server. E.g.:"${HOSTNAME_SHORT}.cfg"
		3. sharedConfig.cfg	for all groups and all servers.
		In addition the library sharedFunctions.bash is required and this one must be located in ${LIB_DIR}.
===============================================================================================================

EOH
	trap - EXIT
	exit 0
}

#
##
### Job controle functions
##
#
function doesTableExist() {
	local _db_path="${1}"
	local _db_table="${2}"

	[[ -f "${_db_path}" ]] || return 1
	[[ -n "${_db_table}" ]] || return 1

	sqlite3 "${_db_path}" \
		"SELECT 1 FROM sqlite_master WHERE type='table' AND name='${_db_table}' LIMIT 1;" \
		| grep -q 1
}

function isAlreadyProcessed() {
	local _datatype="${1}"
	local _job_control_line="${2}"

	local finished_file="${logs_dir}/process.${_datatype}.trendanalysis.finished"
	if [[ -f "${finished_file}" ]]
	then
		grep -Fxq "${_job_control_line}" "${finished_file}"
	else
		touch "${finished_file}"
		return 1
	fi
}

function markProcessingStarted() {
	local _datatype="${1}"
	local _job_control_line="${2}"

	touch "${logs_dir}/process.${_datatype}.trendanalysis."{started,failed,finished}
	echo "${_job_control_line}" >> "${logs_dir}/process.${_datatype}.trendanalysis.started"
}

function markProcessingFinished() {
	local _datatype="${1}"
	local _job_control_line="${2}"

	sed -i "/${_job_control_line}/d" "${logs_dir}/process.${_datatype}.trendanalysis."{started,failed}
	echo "${_job_control_line}" >> "${logs_dir}/process.${_datatype}.trendanalysis.finished"
}

function markProcessingFailed() {
	local _datatype="${1}"
	local _job_control_line="${2}"

	sed -i "/${_job_control_line}/d" "${logs_dir}/process.${_datatype}.trendanalysis.started"
	echo "${_job_control_line}" >> "${logs_dir}/process.${_datatype}.trendanalysis.failed"
}

# Generic data processor for each datahandler/dataType
function processData() {
	local _datatype="${1}"
	local _data_handler="${2}"
	local _basedir="${3}"

	log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Processing of datatype '${_datatype}' started..."
	# Get all runDirs in form the provided $basedir
	readarray -t runs < <(find "${_basedir}" -maxdepth 1 -mindepth 1 -type d -name "[!.]*" -printf "%f\n")

	# If exporting a full dataset and no rundirs are provided,
	# use basename + date as the rundir. For example: ogm and darwin dataType.
	if [[ "${#runs[@]}" -eq 0 ]]; then
		runs=( "$(basename "${_basedir}").${today}" )
	fi

	# Iterate over runs and process each one exactly once, 
	# using job control start/finish/failed states in logfiles per dataType stored in ${logs_dir}.
	for run in "${runs[@]}"; do
		local _job_control_line="${run}.trendanalysis.${_data_handler}"
		
		# shellcheck disable=SC2310
		if isAlreadyProcessed "${_datatype}" "${_job_control_line}"; then
			log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Processing of datatype '${_datatype}' and project '${run}' already done."
			continue
		fi
		markProcessingStarted "${_datatype}" "${_job_control_line}"
		if "${_data_handler}" "${run}" "${_basedir}"; then
			markProcessingFinished "${_datatype}" "${_job_control_line}"
		else
			markProcessingFailed "${_datatype}" "${_job_control_line}"
		fi
	done
}


#
##
### Data proccessing functions.
##
#

function updateOrCreateDatabase() {

	local _db_table="${1}" #SequenceRun
	local _tableFile="${2}" #"${chronqc_tmp}/${_rawdata}.SequenceRun.csv"
	local _runDateInfo="${3}" #"${chronqc_tmp}/${_rawdata}.SequenceRun_run_date_info.csv"
	local _dataLabel="${4}" #"${_sequencer}" 
	local _forceCreate="${5:-false}"
	
	log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Force create database for project == ${_forceCreate}"
	
	# shellcheck disable=SC2310
	if doesTableExist "${CHRONQC_DATABASE_NAME}/chronqc_db/chronqc.stats.sqlite" "${_db_table}" || [[ "${_forceCreate}" != "true" ]]; then
		log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Update database for project ${_tableFile} in exiting table ${_db_table}."
		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Tabel ${_db_table} does exist in ${CHRONQC_DATABASE_NAME}/chronqc_db/chronqc.stats.sqlite" 
		# update datebase if tabel already exist.
		chronqc database --update --db "${CHRONQC_DATABASE_NAME}/chronqc_db/chronqc.stats.sqlite" \
				"${_tableFile}" \
				--db-table "${_db_table}" \
				--run-date-info "${_runDateInfo}" \
				"${_dataLabel}" || {
					log4Bash 'ERROR' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "Failed to import ${_tableFile} with ${_dataLabel} stored to Chronqc database." 
					return 1
		}
	else
		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Tabel ${_db_table} does not exist in ${CHRONQC_DATABASE_NAME}/chronqc_db/chronqc.stats.sqlite, \
				or _forceCreate. == true"
		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Create database for project ${_tableFile}"
		# Created non existing table, and adds new rundata. Or _forceCreate table when _forceCreate == true"
		chronqc database --create -f \
			-o "${CHRONQC_DATABASE_NAME}" \
			"${_tableFile}" \
			--run-date-info "${_runDateInfo}" \
			--db-table "${_db_table}" \
			"${_dataLabel}" -f || {
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "Failed to create database and import ${_tableFile} with ${_dataLabel} stored to Chronqc database." 
			return 1
			}
		fi
	log4Bash 'INFO' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "${FUNCNAME[0]} ${_tableFile} with ${_dataLabel} was stored in Chronqc database."
}

function processProjects() {
	local _project="${1}"
	local _chronqc_projects_dir="${2}"
	_chronqc_projects_dir="${_chronqc_projects_dir}/${_project}"

	log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Removing files from ${chronqc_tmp} ..."
	rm -rf "${chronqc_tmp:-missing}"/*

	log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Processing ${_chronqc_projects_dir}/${_project}.run_date_info.csv"
	if [[ -e "${_chronqc_projects_dir}/${_project}.run_date_info.csv" ]]
	then
		log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "found ${_chronqc_projects_dir}/${_project}.run_date_info.csv. Pre-processing: ${_project}."
		cp "${_chronqc_projects_dir}/${_project}.run_date_info.csv" "${chronqc_tmp}/${_project}.run_date_info.csv"
		cp "${_chronqc_projects_dir}/multiqc_sources.txt" "${chronqc_tmp}/${_project}.multiqc_sources.txt"
		for multiQC in "${MULTIQC_METRICS_TO_PLOT[@]}"
		do
			local _metrics="${multiQC%:*}"
			log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "using _metrics: ${_metrics}"
			if [[ "${_metrics}" == multiqc_picard_insertSize.txt ]]
			then
				cp "${_chronqc_projects_dir}/${_metrics}" "${chronqc_tmp}/${_project}.${_metrics}"
				awk -v FS='\t' '{$1=""}1' "${chronqc_tmp}/${_project}.${_metrics}" | awk -v OFS='\t' '{$1=$1}1' > "${chronqc_tmp}/${_project}.1.${_metrics}"
				perl -pe 's|SAMPLE_NAME\t|Sample\t|' "${chronqc_tmp}/${_project}.1.${_metrics}" > "${chronqc_tmp}/${_project}.3.${_metrics}"
				perl -pe 's|SAMPLE\t|SAMPLE_NAME2\t|' "${chronqc_tmp}/${_project}.3.${_metrics}" > "${chronqc_tmp}/${_project}.2.${_metrics}"
			elif [[ "${_metrics}" == multiqc_fastqc.txt ]]
			then
				cp "${_chronqc_projects_dir}/${_metrics}" "${chronqc_tmp}/${_project}.${_metrics}"
				# This part will make a run_date_info.csv for only the lane information
				echo -e 'Sample,Run,Date' >> "${chronqc_tmp}/${_project}.lane.run_date_info.csv"
				IFS=$'\t' read -ra perLaneSample <<< "$(awk '$1 ~ /.recoded/ {print $1}' "${chronqc_tmp}/${_project}.${_metrics}" | tr '\n' '\t')"

				for laneSample in "${perLaneSample[@]}"
				do
					runDate=$(echo "${laneSample}" | cut -d "_" -f 1)
					echo -e "${laneSample},${_project},${runDate}" >> "${chronqc_tmp}/${_project}.lane.run_date_info.csv"
				done
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "using _metrics: ${_metrics} to create ${_project}.lane.run_date_info.csv"
				echo -e 'Sample\t%GC\ttotal_deduplicated_percentage' >> "${chronqc_tmp}/${_project}.2.${_metrics}"
				awk -v FS="\t" -v OFS='\t' -v header="Sample,%GC,total_deduplicated_percentage" 'FNR==1{split(header,h,/,/);for(i=1; i in h; i++){for(j=1; j<=NF; j++){if(tolower(h[i])==tolower($j)){ d[i]=j; break }}}next}{for(i=1; i in h; i++)printf("%s%s",i>1 ? OFS:"",  i in d ?$(d[i]):"");print "";}' "${chronqc_tmp}/${_project}.${_metrics}" >> "${chronqc_tmp}/${_project}.2.${_metrics}"
			else
				cp "${_chronqc_projects_dir}/${_metrics}" "${chronqc_tmp}/${_project}.${_metrics}"
				perl -pe 's|SAMPLE\t|SAMPLE_NAME2\t|' "${chronqc_tmp}/${_project}.${_metrics}" > "${chronqc_tmp}/${_project}.2.${_metrics}"
			fi
		done
		#
		# Rename one of the duplicated SAMPLE column names to make it work.
		#
		cp "${chronqc_tmp}/${_project}.run_date_info.csv" "${chronqc_tmp}/${_project}.2.run_date_info.csv"

		#
		# Get all the samples processed with FastQC form the MultiQC multi_source file,
		# because samplenames differ from regular samplesheet at that stage in th epipeline.
		# The Output is converted into standard ChronQC run_date_info.csv format.
		#
		awk 'BEGIN{FS=OFS=","} NR>1{cmd = "date -d \"" $3 "\" \"+%d/%m/%Y\"";cmd | getline out; $3=out; close("uuidgen")} 1' "${chronqc_tmp}/${_project}.2.run_date_info.csv" > "${chronqc_tmp}/${_project}.2.run_date_info.csv.tmp"
		awk 'BEGIN{FS=OFS=","} NR>1{cmd = "date -d \"" $3 "\" \"+%d/%m/%Y\"";cmd | getline out; $3=out; close("uuidgen")} 1' "${chronqc_tmp}/${_project}.lane.run_date_info.csv" > "${chronqc_tmp}/${_project}.lane.run_date_info.csv.tmp"

		#
		# Check if the date in the run_date_info.csv file is in correct format, dd/mm/yyyy
		#
		_checkdate=$(awk 'BEGIN{FS=OFS=","} NR==2 {print $3}' "${chronqc_tmp}/${_project}.2.run_date_info.csv.tmp")
		log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "_checkdate:${_checkdate}"
		mv "${chronqc_tmp}/${_project}.2.run_date_info.csv.tmp" "${chronqc_tmp}/${_project}.2.run_date_info.csv"
		_checkdate=$(awk 'BEGIN{FS=OFS=","} NR==2 {print $3}' "${chronqc_tmp}/${_project}.lane.run_date_info.csv.tmp")
		log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "_checkdate:${_checkdate}"
		mv "${chronqc_tmp}/${_project}.lane.run_date_info.csv.tmp" "${chronqc_tmp}/${_project}.lane.run_date_info.csv"

		#
		# Get panel information from $_project} based on column 'capturingKit'.
		#
		_panel=$(awk -F "${SAMPLESHEET_SEP}" 'NR==1 { for (i=1; i<=NF; i++) { f[$i] = i}}{if(NR > 1) print $(f["capturingKit"]) }' "${_chronqc_projects_dir}/${_project}.csv" | sort -u | cut -d'/' -f2)
		IFS='_' read -r -a array <<< "${_panel}"
		if [[ "${array[0]}" == *"Exoom"* ]]
		then
			_panel='Exoom'
		else
			_panel="${array[0]}"
		fi
		log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "PANEL= ${_panel}"
		if [[ "${_checkdate}"  =~ [0-9] ]]
		then
			for i in "${MULTIQC_METRICS_TO_PLOT[@]}"
			do
				local _metrics="${i%:*}"
				local _table="${i#*:}"
				log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Importing ${_project}.${_metrics}, and using table ${_table}"
				if [[ "${_metrics}" == multiqc_fastqc.txt ]]
				then
					updateOrCreateDatabase "${_table}" "${chronqc_tmp}/${_project}.2.${_metrics}" "${chronqc_tmp}/${_project}.lane.run_date_info.csv" "${_panel}" || {
					echo 'bla'
					return 1 
					}
				elif [[ -f "${chronqc_tmp}/${_project}.2.${_metrics}" ]]
				then
					updateOrCreateDatabase "${_table}" "${chronqc_tmp}/${_project}.2.${_metrics}" "${chronqc_tmp}/${_project}.2.run_date_info.csv" "${_panel}" || return 1
				else
					log4Bash 'INFO' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "The file ${chronqc_tmp}/${_project}.2.${_metrics} does not exist, so can't be added to the database"
					continue
				fi
			done
		else
			log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "${_project}: panel: ${_panel} has date ${_checkdate} this is not fit for chronQC." 
			return 1
		fi
	else
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "For project ${_project} no run date info file is present, ${_project} cant be added to the database."
	fi
}

function processRnaProjects {
	local _rnaproject="${1}"
	local _chronqc_rnaprojects_dir="${2}"
	_chronqc_rnaprojects_dir="${_chronqc_rnaprojects_dir}/${_rnaproject}"
	
	log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Removing files from ${chronqc_tmp} ..."
	rm -rf "${chronqc_tmp:-missing}"/*

	log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Processing ${_chronqc_rnaprojects_dir}/${_rnaproject}.run_date_info.csv"
	if [[ -e "${_chronqc_rnaprojects_dir}/${_rnaproject}.run_date_info.csv" ]]
	then
		log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "found ${_chronqc_rnaprojects_dir}/${_rnaproject}.run_date_info.csv. Updating ChronQC database with ${_rnaproject}."
		cp "${_chronqc_rnaprojects_dir}/${_rnaproject}.run_date_info.csv" "${chronqc_tmp}/${_rnaproject}.run_date_info.csv"
		for RNAmultiQC in "${MULTIQC_RNA_METRICS_TO_PLOT[@]}"
		do
	#'multiqc_general_stats.txt:general_stats'
	#'multiqc_star.txt:star'
	#'multiqc_picard_RnaSeqMetrics.txt:RnaSeqMetrics'

			local _rnametrics="${RNAmultiQC%:*}"
			log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "using _rnametrics: ${_rnametrics}"
			if [[ "${_rnametrics}" == multiqc_picard_RnaSeqMetrics.txt ]]
			then
				# Rename one of the duplicated SAMPLE column names to make it work.
				cp "${_chronqc_rnaprojects_dir}/${_rnametrics}" "${chronqc_tmp}/${_rnaproject}.${_rnametrics}"
				perl -pe 's|SAMPLE\t|SAMPLE_NAME2\t|' "${chronqc_tmp}/${_rnaproject}.${_rnametrics}" > "${chronqc_tmp}/${_rnaproject}.1.${_rnametrics}"
			else
				cp "${_chronqc_rnaprojects_dir}/${_rnametrics}" "${chronqc_tmp}/${_rnaproject}.${_rnametrics}"
			fi
		done
		#
		# Get all the samples processed with FastQC form the MultiQC multi_source file,
		# because samplenames differ from regular samplesheet at that stage in th epipeline.
		# The Output is converted into standard ChronQC run_date_info.csv format.
		#
		awk 'BEGIN{FS=OFS=","} NR>1{cmd = "date -d \"" $3 "\" \"+%d/%m/%Y\"";cmd | getline out; $3=out; close("uuidgen")} 1' "${chronqc_tmp}/${_rnaproject}.run_date_info.csv" > "${chronqc_tmp}/${_rnaproject}.2.run_date_info.csv"

		#
		# Check if the date in the run_date_info.csv file is in correct format, dd/mm/yyyy
		#
		_checkdate=$(awk 'BEGIN{FS=OFS=","} NR==2 {print $3}' "${chronqc_tmp}/${_rnaproject}.run_date_info.csv")
		log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "_checkdate:${_checkdate}"
		
		if [[ "${_checkdate}"  =~ [0-9] ]]
		then
			for i in "${MULTIQC_RNA_METRICS_TO_PLOT[@]}"
			do
				local _rnametrics="${i%:*}"
				local _rnatable="${i#*:}"
				log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Importing ${_rnaproject}.${_rnametrics}, and using table ${_rnatable}"
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "________________${_rnametrics}________${_rnatable}_____________"
				if [[ "${_rnametrics}" == multiqc_picard_RnaSeqMetrics.txt ]]
				then
					updateOrCreateDatabase "${_rnatable}" "${chronqc_tmp}/${_rnaproject}.1.${_rnametrics}" "${chronqc_tmp}/${_rnaproject}.2.run_date_info.csv" RNA || return 1
				else
					updateOrCreateDatabase "${_rnatable}" "${chronqc_tmp}/${_rnaproject}.${_rnametrics}" "${chronqc_tmp}/${_rnaproject}.2.run_date_info.csv" RNA || return 1
				fi
			done
		else
			log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "${_rnaproject}: has date ${_checkdate} this is not fit for chronQC." 
			return 1
		fi
	else
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "For project ${_rnaproject} no run date info file is present, ${_rnaproject} cant be added to the database."
	fi
}

function processDarwin() {
	local _darwin_project="${1}"
	local _darwin_dir="${2}"
	_chronqc_darwin_dir="${_darwin_dir}/"

	log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Removing files from ${chronqc_tmp} ..."
	rm -rf "${chronqc_tmp:-missing}"/*

	readarray -t darwindata < <(
		find "${_chronqc_darwin_dir}" \
			-maxdepth 1 \
			-mindepth 1 \
			-type f \
			-name "*runinfo*.csv" \
			-printf '%f\n'
	)

	if [[ "${#darwindata[@]}" -eq 0 ]]; then
		log4Bash 'WARN' "${LINENO}" "${FUNCNAME[0]}" '0' \
			"No Darwin runinfo files found in ${_chronqc_darwin_dir}"
		return 0
	fi

	for darwinfile in "${darwindata[@]}"; do
		
		_runInfo="$(basename "${darwinfile}" .csv)"
		_fileType="$(cut -d '_' -f1 <<< "${_runInfo}")"
		_fileDate="$(cut -d '_' -f3 <<< "${_runInfo}")"
		_tableFile="${_chronqc_darwin_dir}/${_fileType}_${_fileDate}.csv"
		_runInfoFile="${_chronqc_darwin_dir}/${darwinfile}"

		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "local variables generateChronQCOutput:_runinfo=${_runInfo},_tablefile=${_tableFile}, _filetype=${_fileType}, _fileDate=${_fileDate}"
		log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "starting to file the trendanalysis database with :${_runInfo} and ${_tableFile}"

		if [[ "${_fileType}" == 'Concentratie' ]]
		then
			# for now the database will be filled with only the concentration information from the Nimbus2000
			head -1 "${_runInfoFile}" > "${chronqc_tmp}/ConcentratieNimbus_runinfo_${_fileDate}.csv"
			head -1 "${_tableFile}" > "${chronqc_tmp}/ConcentratieNimbus_${_fileDate}.csv"

			grep Nimbus "${_runInfoFile}" >> "${chronqc_tmp}/ConcentratieNimbus_runinfo_${_fileDate}.csv"
			grep Nimbus "${_tableFile}" >> "${chronqc_tmp}/ConcentratieNimbus_${_fileDate}.csv"
			
			updateOrCreateDatabase "${_fileType}" "${chronqc_tmp}/ConcentratieNimbus_${_fileDate}.csv" "${chronqc_tmp}/ConcentratieNimbus_runinfo_${_fileDate}.csv" Nimbus true || return 1

			log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "database filled with ConcentratieNimbus_${_fileDate}.csv"
		else
			updateOrCreateDatabase "${_fileType}" "${_tableFile}" "${_runInfoFile}" NGSlab true || return 1
		fi
	done
}

function processOpenArray() {

	local _openarrayproject="${1}"
	local _openarrayprojectdir
	local _openarrayfile="${_openarrayproject}.txt"
	local _chronqc_openarray_dir="${tmp_trendanalyse_dir}/openarray/"
	_openarrayprojectdir="${_chronqc_openarray_dir}/${_openarrayproject}/"
	
	rm -rf "${chronqc_tmp:-missing}"/*

	log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "_openarrayprojectdir is: ${_openarrayprojectdir}."
	log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "_openarrayproject is: ${_openarrayproject}."
	log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "_openarrayfile is: ${_openarrayfile}."
	
	if [[ -e "${_openarrayprojectdir}/${_openarrayproject}.txt" ]]
	then
		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "processing ${_openarrayfile}"
		dos2unix "${_chronqc_openarray_dir}/${_openarrayproject}/${_openarrayproject}.txt"
	
		project=$(grep '# Study Name : ' "${_openarrayprojectdir}/${_openarrayproject}.txt" | awk 'BEGIN{FS=" "}{print $5}')
		year=$(grep  '# Export Date : ' "${_openarrayprojectdir}/${_openarrayproject}.txt" | awk 'BEGIN{FS=" "}{print $5}' | awk 'BEGIN{FS="/"}{print $3}')
		month=$(grep  '# Export Date : ' "${_openarrayprojectdir}/${_openarrayproject}.txt" | awk 'BEGIN{FS=" "}{print $5}' | awk 'BEGIN{FS="/"}{print $1}')
		day=$(grep  '# Export Date : ' "${_openarrayprojectdir}/${_openarrayproject}.txt" | awk 'BEGIN{FS=" "}{print $5}' | awk 'BEGIN{FS="/"}{print $2}')
	
		date="${day}/${month}/${year}"
	
		#select snps, and flag snps with SD > 80% as PASS.
		awk '/Assay Name/,/Experiment Name/ {
			sub("%$","",$3); {
			if ($3+0 > 75.0 ) {
				print $1"\t"$2"\t"$3"\tPASS"}
			else {
				print $1"\t"$2"\t"$3"\tFAIL" }
				}
			}' "${_openarrayprojectdir}/${_openarrayproject}.txt" > "${_openarrayprojectdir}/${_openarrayproject}.snps.csv"
	
		# remove last two rows, and replace header.
		head -n -2 "${_openarrayprojectdir}/${_openarrayproject}.snps.csv" > "${chronqc_tmp}/${_openarrayproject}.snps.csv.temp" 
		sed '1 s/.*/Sample\tAssay ID\tAssay Call Rate\tQC_PASS/' "${chronqc_tmp}/${_openarrayproject}.snps.csv.temp" > "${_openarrayprojectdir}/${_openarrayproject}.snps.csv"
	
		#create ChronQC snp samplesheet
		echo -e "Sample,Run,Date" > "${_openarrayprojectdir}/${_openarrayproject}.snps.run_date_info.csv"
		tail -n +2 "${_openarrayprojectdir}/${_openarrayproject}.snps.csv" | awk -v project="${project}"  -v date="${date}" '{ print $1","project","date }' >> "${_openarrayprojectdir}/${_openarrayproject}.snps.run_date_info.csv"
	
		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "generated ${_openarrayprojectdir}/${_openarrayproject}.snps.run_date_info.csv"
	
		#create project.run.csv
		awk '/Experiment Name/,/Sample ID/' "${_openarrayprojectdir}/${_openarrayproject}.txt" > "${chronqc_tmp}/${_openarrayproject}.run.csv.temp"
		head -n -2 "${chronqc_tmp}/${_openarrayproject}.run.csv.temp" > "${_openarrayprojectdir}/${_openarrayproject}.run.csv"
		perl -pi -e 's|Experiment Name|Sample|' "${_openarrayprojectdir}/${_openarrayproject}.run.csv"
		perl -pi -e 's|\%||g' "${_openarrayprojectdir}/${_openarrayproject}.run.csv"
		sed "2s/\.*[^ \t]*/${project}/" "${_openarrayprojectdir}/${_openarrayproject}.run.csv" > "${chronqc_tmp}/${_openarrayproject}.run.csv.temp"
		mv "${chronqc_tmp}/${_openarrayproject}.run.csv.temp" "${_openarrayprojectdir}/${_openarrayproject}.run.csv"
	
		#create ChronQC runSD samplesheet
		echo -e "Sample,Run,Date" > "${_openarrayprojectdir}/${_openarrayproject}.run.run_date_info.csv"
		echo -e "${project},${project},${date}" >> "${_openarrayprojectdir}/${_openarrayproject}.run.run_date_info.csv"
	
		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "generated ${_openarrayprojectdir}/${_openarrayproject}.run.run_date_info.csv"
	
		#create project.sample.csv file, and flag samples with SD > 80% as PASS.
		awk '/Sample ID/,/^$/ {
			sub("%$","",$2); {
			if ($2+0 > 75 ) {
				print $1"\t"$2"\tPASS"}
			else {
				print $1"\t"$2"\tFAIL" }
				}
			}' "${_openarrayprojectdir}/${_openarrayproject}.txt" > "${_openarrayprojectdir}/${_openarrayproject}.samples.csv"
	
		# remove last line, and replace header.
		head -n -1 "${_openarrayprojectdir}/${_openarrayproject}.samples.csv" > "${chronqc_tmp}/${_openarrayproject}.samples.csv.temp" 
		sed '1 s/.*/Sample\tSample Call Rate\tQC_PASS/' "${chronqc_tmp}/${_openarrayproject}.samples.csv.temp" > "${_openarrayprojectdir}/${_openarrayproject}.samples.csv"
	
		#create ChronQC sample samplesheet.
		echo -e "Sample,Run,Date" > "${_openarrayprojectdir}/${_openarrayproject}.samples.run_date_info.csv"
		tail -n +2 "${_openarrayprojectdir}/${_openarrayproject}.samples.csv" | awk -v project="${project}"  -v date="${date}" '{ print $1","project","date }' >> "${_openarrayprojectdir}/${_openarrayproject}.samples.run_date_info.csv"
	
		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "generated ${_openarrayprojectdir}/${_openarrayproject}.samples.run_date_info.csv"
		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "__________________function processOpenArray is done___________________"

		updateOrCreateDatabase run "${_openarrayprojectdir}/${_openarrayproject}.run.csv" "${_openarrayprojectdir}/${_openarrayproject}.run.run_date_info.csv" openarray || return 1
		updateOrCreateDatabase samples "${_openarrayprojectdir}/${_openarrayproject}.samples.csv" "${_openarrayprojectdir}/${_openarrayproject}.samples.run_date_info.csv" openarray || return 1
		updateOrCreateDatabase snps "${_openarrayprojectdir}/${_openarrayproject}.snps.csv" "${_openarrayprojectdir}/${_openarrayproject}.snps.run_date_info.csv" openarray || return 1
		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "done updating the database with ${_openarrayproject}"
	else
		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "Project: ${_openarrayprojectdir}/${_openarrayproject} is not accrording to standard formatting, skipping"
		
	fi
}

function processOGM() {
	local _maindir="${1}"
	local _ogm_input_dir="${2}"
	local _ogm_dir="${_ogm_input_dir%/*}"

	readarray -t ogmdata < <(find "${_ogm_input_dir}" -maxdepth 1 -mindepth 1 -type f -name "bas*" | sed -e "s|^${_ogm_input_dir}/||")
	if [[ "${#ogmdata[@]}" -eq '0' ]]
	then
		log4Bash 'WARN' "${LINENO}" "${FUNCNAME:-main}" '0' "No projects found @ ${_ogm_input_dir}."
	else
		for ogmcsvfile in "${ogmdata[@]}"
		do
			ogmfilename=$(basename "${ogmcsvfile}" .csv)
			ogmfile="${_ogm_input_dir}/${ogmcsvfile}"
			headercheck='Chip run uid,Flow cell,Instrument,Total DNA (>= 150Kbp),N50 (>= 150Kbp),Average label density (>= 150Kbp),Map rate (%),DNA per scan (Gbp),Longest molecule (Kbp),Timestamp'
			headerogmfile=$(head -1 "${ogmfile}")
			if [[ "${headercheck}" == "${headerogmfile}" ]]
			then
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "The header of ${ogmcsvfile} is in the correct format."
				basmachine=$(echo "${ogmfilename}" | cut -d '.' -f1)
				mainfile="${_ogm_dir}/mainMetrics-${basmachine}.csv"
				touch "${mainfile}"
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "starting on ogmcsvfile ${ogmcsvfile}."
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "basmachine: ${basmachine}"
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "adding ${ogmfilename} to ${mainfile}."
				tail -n +2 "${mainfile}" > "${mainfile}.tmp"
				tail -n +2 "${ogmfile}" > "${ogmfile}.tmp"
				metricsfiletoday="${tmp_trendanalyse_dir}/ogm/metricsFile_${today}.csv"
				mainHeader=$(head -1 "${ogmfile}")
				echo -e "${mainHeader}" > "${metricsfiletoday}"
				sort -u "${mainfile}.tmp" "${ogmfile}.tmp" >> "${metricsfiletoday}"
				rm "${mainfile}"
				rm "${mainfile}.tmp"
				rm "${ogmfile}.tmp"
				cp "${metricsfiletoday}" "${mainfile}"
				mv "${metricsfiletoday}" "${tmp_trendanalyse_dir}/ogm/metricsFinished/"
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "done creating new ${mainfile} added ${ogmfile}"
			else
				log4Bash 'WARN' "${LINENO}" "${FUNCNAME:-main}" '0' "The header of ${ogmcsvfile} is in the wrong format."
			fi
		done
	
		readarray -t mainogmdata< <(find "${_ogm_dir}" -maxdepth 1 -mindepth 1 -type f -name "mainMetrics*")

		if [[ "${#mainogmdata[@]}" -eq '0' ]]
		then
			log4Bash 'WARN' "${LINENO}" "${FUNCNAME:-main}" '0' "No mainMetrics file found @ ${_ogm_dir}."
		else
			for mainbasfile in "${mainogmdata[@]}"
			do
				baslabel=$(basename "${mainbasfile}" .csv | cut -d '-' -f2)
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Starting on ogm file ${mainbasfile}, adding it to the database."
				
				declare -a statsFileColumnNames=()
				declare -A statsFileColumnOffsets=()

				IFS=',' read -r -a statsFileColumnNames <<< "$(head -1 "${mainbasfile}")"
				
				for (( offset = 0 ; offset < ${#statsFileColumnNames[@]} ; offset++ ))
				do
					columnName="${statsFileColumnNames[${offset}]}"
					statsFileColumnOffsets["${columnName}"]="${offset}"
				done

				chipRunUIDFieldIndex=$((${statsFileColumnOffsets['Chip run uid']} + 1))
				FlowCellFielIndex=$((${statsFileColumnOffsets['Flow cell']} + 1))
				TotalDNAFieldIndex=$((${statsFileColumnOffsets['Total DNA (>= 150Kbp)']} + 1))
				N50FieldIndex=$((${statsFileColumnOffsets['N50 (>= 150Kbp)']} + 1))
				AverageLabelDensityFieldIndex=$((${statsFileColumnOffsets['Average label density (>= 150Kbp)']} + 1))
				MapRateFieldIndex=$((${statsFileColumnOffsets['Map rate (%)']} + 1))
				DNAPerScanFieldIndex=$((${statsFileColumnOffsets['DNA per scan (Gbp)']} + 1))
				LongestMolecuulFieldIndex=$((${statsFileColumnOffsets['Longest molecule (Kbp)']} + 1))
				TimeStampFieldIndex=$((${statsFileColumnOffsets['Timestamp']} + 1))

				echo -e 'Sample,Run,Date' > "${_ogm_dir}/OGM-${baslabel}_runDateInfo_${today}.csv"

				while read -r line
				do
						dateField=$(echo "${line}" | cut -d ',' -f "${TimeStampFieldIndex}")
						sampleField=$(echo "${line}" | cut -d ',' -f "${chipRunUIDFieldIndex}")
						runField=$(echo "${line}" | cut -d ',' -f "${FlowCellFielIndex}")
						correctDate=$(date -d "${dateField}" '+%d/%m/%Y')
						echo -e "${sampleField},${runField},${correctDate}" >> "${_ogm_dir}/OGM-${baslabel}_runDateInfo_${today}.csv"
				done < <(tail -n +2 "${mainbasfile}")

				echo -e 'Sample\tFlow_cell\tTotal_DNA(>=150Kbp)\tN50(>=150Kbp)\tAverage_label_density(>=150Kbp)\tMap_rate(%)\tDNA_per_scan(Gbp)\tLongest_molecule(Kbp)' > "${_ogm_dir}/OGM-${baslabel}_${today}.csv"
				awk -v s="${chipRunUIDFieldIndex}" \
						-v s1="${FlowCellFielIndex}" \
						-v s2="${TotalDNAFieldIndex}" \
						-v s3="${N50FieldIndex}" \
						-v s4="${AverageLabelDensityFieldIndex}" \
						-v s5="${MapRateFieldIndex}" \
						-v s6="${DNAPerScanFieldIndex}" \
						-v s7="${LongestMolecuulFieldIndex}" \
						'BEGIN {FS=","}{OFS="\t"}{if (NR>1){print $s,$s1,$s2,$s3,$s4,$s5,$s6,$s7}}' "${mainbasfile}" >> "${_ogm_dir}/OGM-${baslabel}_${today}.csv"

				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "starting to update or create database using OGM-${baslabel}_${today}.csv and OGM-${baslabel}_runDateInfo_${today}.csv"
				# force create a new table with forcecreate == true
				updateOrCreateDatabase "${baslabel}" "${_ogm_dir}/OGM-${baslabel}_${today}.csv" "${_ogm_dir}/OGM-${baslabel}_runDateInfo_${today}.csv" "${baslabel}" true || return 1
				mv "${_ogm_dir}/OGM-${baslabel}_${today}.csv" "${_ogm_dir}/metricsFinished/"
				mv "${_ogm_dir}/OGM-${baslabel}_runDateInfo_${today}.csv" "${_ogm_dir}/metricsFinished/"
			done
		fi
	fi
}

function processRawdata(){
	local _rawdata="${1}"
	local _chronqc_rawdata_dir="${2}"
	_chronqc_rawdata_dir="${_chronqc_rawdata_dir}/${_rawdata}"

	log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Removing files from ${chronqc_tmp} ..."
	rm -rf "${chronqc_tmp:-missing}"/*

	log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Processing ${_chronqc_rawdata_dir}/SequenceRun_run_date_info.csv"
	log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "New batch ${_rawdata} will be processed."
	sequencer=$(echo "${_rawdata}" | cut -d '_' -f2)

	if [[ -e "${_chronqc_rawdata_dir}/SequenceRun_run_date_info.csv" ]]
	then
		cp "${_chronqc_rawdata_dir}/SequenceRun_run_date_info.csv" "${chronqc_tmp}/${_rawdata}.SequenceRun_run_date_info.csv"
		cp "${_chronqc_rawdata_dir}/SequenceRun.csv" "${chronqc_tmp}/${_rawdata}.SequenceRun.csv"
		updateOrCreateDatabase SequenceRun "${chronqc_tmp}/${_rawdata}.SequenceRun.csv" "${chronqc_tmp}/${_rawdata}.SequenceRun_run_date_info.csv" "${sequencer}" || return 1
	else
		log4Bash 'INFO' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "${FUNCNAME[0]} for sequence run ${_rawdata}, no sequencer statistics were stored "
	fi
}

function processDragen() {

	local _dragenProject="${1}"
	local _dragenProjectDir="${2}"
	_dragenProjectDir="${_dragenProjectDir}/${_dragenProject}"
	_dataType=$(echo "${_dragenProject}" | cut -d '_' -f2 | cut -d '-' -f2)

	
	log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Processing ${_dragenProject}"
	
	declare -a statsFileColumnNames=()
	declare -A statsFileColumnOffsets=()
	
	if [[ "${_dataType}" == 'Exoom' ]]
	then
		log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Skip data pre-processing for Exoom data for run: ${_dragenProject}"
	else
		IFS=$'\t' read -r -a statsFileColumnNames <<< "$(head -1 "${_dragenProjectDir}/${_dragenProject}".stats.tsv)"
		
		for (( offset = 0 ; offset < ${#statsFileColumnNames[@]} ; offset++ ))
		do
			columnName="${statsFileColumnNames[${offset}]}"
			statsFileColumnOffsets["${columnName}"]="${offset}"
		done
		
		sampleNameFieldIndex=$((${statsFileColumnOffsets['sample_name']} + 1))
		totalBasesFieldIndex=$((${statsFileColumnOffsets['total_bases']} + 1))
		totalReadsFieldIndex=$((${statsFileColumnOffsets['total_reads']} + 1))
		hq_MappedreadsFieldIndex=$((${statsFileColumnOffsets['hq_mapped_reads']} + 1))
		duplicateReadPairsFieldIndex=$((${statsFileColumnOffsets['duplicate_readpairs']} + 1))
		basesOnTargetFieldIndex=$((${statsFileColumnOffsets['bases_on_target']} + 1))
		meanInsertSizeFieldIndex=$((${statsFileColumnOffsets['mean_insert_size']} + 1))
		fracMin1xCoverageFieldIndex=$((${statsFileColumnOffsets['frac_min_1x_coverage']} + 1))
		
		if [[ -n "${statsFileColumnOffsets['frac_duplicates']+isset}" ]]
		then
			fracDuplicatesFieldIndex=$((${statsFileColumnOffsets['frac_duplicates']} + 1))
		fi
		if [[ -n "${statsFileColumnOffsets['mean_coverage_genome']+isset}" ]]
		then
			meanCoverageGenomeFieldIndex=$((${statsFileColumnOffsets['mean_coverage_genome']} + 1))
		fi
		if [[ -n "${statsFileColumnOffsets['frac_min_10x_coverage']+isset}" ]]
		then
			fracMin10xCoverageFieldIndex=$((${statsFileColumnOffsets['frac_min_10x_coverage']} + 1))
		fi
		if [[ -n "${statsFileColumnOffsets['frac_min_50x_coverage']+isset}" ]]
		then
			fracMin50xCoverageFieldIndex=$((${statsFileColumnOffsets['frac_min_50x_coverage']} + 1))
		fi
		if [[ -n "${statsFileColumnOffsets['mean_alignment_coverage']+isset}" ]]
		then
			mean_alignment_coverageCoverageFieldIndex=$((${statsFileColumnOffsets['mean_alignment_coverage']} + 1))
		fi
		if [[ -n "${statsFileColumnOffsets['coverage_uniformity']+isset}" ]]
		then
			coverage_uniformityCoverageFieldIndex=$((${statsFileColumnOffsets['coverage_uniformity']} + 1))
		fi

		file_date=$(date -r "${_dragenProjectDir}/${_dragenProject}.stats.tsv" '+%d/%m/%Y')
		echo -e 'Sample,Run,Date' > "${_dragenProjectDir}/${_dragenProject}.Dragen_runinfo.csv"
		awk -v s="${_dragenProject}" -v f="${file_date}" 'BEGIN {FS="\t"}{OFS=","}{if (NR>1){print $1,s,f}}' "${_dragenProjectDir}/${_dragenProject}.stats.tsv" >> "${_dragenProjectDir}/${_dragenProject}.Dragen_runinfo.csv"
		
		if [[ "${_dataType}" == *"sWGS"* ]]
		then
			log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Processing ${_dragenProject} with data type ${_dataType}"
			echo -e 'Sample\tBatchName\ttotal_bases\ttotal_reads\thq_mapped_reads\tduplicate_readpairs\tbases_on_target\tmean_insert_size\tfrac_min_1x_coverage\tfrac_duplicates\tmean_coverage_genome'  > "${_dragenProjectDir}/${_dragenProject}.Dragen.csv"
		
			awk -v s1="${sampleNameFieldIndex}" \
					-v s="${_dragenProject}" \
					-v s2="${totalBasesFieldIndex}" \
					-v s3="${totalReadsFieldIndex}" \
					-v s4="${hq_MappedreadsFieldIndex}" \
					-v s5="${duplicateReadPairsFieldIndex}" \
					-v s6="${basesOnTargetFieldIndex}" \
					-v s7="${meanInsertSizeFieldIndex}" \
					-v s8="${fracMin1xCoverageFieldIndex}" \
					-v s9="${fracDuplicatesFieldIndex}" \
					-v s10="${meanCoverageGenomeFieldIndex}" \
				'BEGIN {FS="\t"}{OFS="\t"}{if (NR>1){print $s1,s,$s2,$s3,$s4,$s5,$s6,$s7,$s8,$s9,$s10}}' "${_dragenProjectDir}/${_dragenProject}.stats.tsv" >>  "${_dragenProjectDir}/${_dragenProject}.Dragen.csv"
		elif [[ "${_dataType}" == *"WGS"* ]]
		then
			log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Processing ${_dragenProject} with data type ${_dataType}"
			echo -e 'Sample\tBatchName\ttotal_bases\ttotal_reads\thq_mapped_reads\tduplicate_readpairs\tbases_on_target\tmean_insert_size\tfrac_min_1x_coverage\tfrac_min_10x_coverage\tfrac_min_50x_coverage\tmean_coverage_genome\tmean_alignment_coverage\tcoverage_uniformity'  > "${_dragenProjectDir}/${_dragenProject}.Dragen.csv"
		
			awk -v s1="${sampleNameFieldIndex}" \
				-v s="${_dragenProject}" \
				-v s2="${totalBasesFieldIndex}" \
				-v s3="${totalReadsFieldIndex}" \
				-v s4="${hq_MappedreadsFieldIndex}" \
				-v s5="${duplicateReadPairsFieldIndex}" \
				-v s6="${basesOnTargetFieldIndex}" \
				-v s7="${meanInsertSizeFieldIndex}" \
				-v s8="${fracMin1xCoverageFieldIndex}" \
				-v s9="${fracMin10xCoverageFieldIndex}" \
				-v s10="${fracMin50xCoverageFieldIndex}" \
				-v s11="${meanCoverageGenomeFieldIndex}" \
				-v s12="${mean_alignment_coverageCoverageFieldIndex}" \
				-v s13="${coverage_uniformityCoverageFieldIndex}" \
			'BEGIN {FS="\t"}{OFS="\t"}{if (NR>1){print $s1,s,$s2,$s3,$s4,$s5,$s6,$s7,$s8,$s9,$s10,$s11,$s12,$s13}}' "${_dragenProjectDir}/${_dragenProject}.stats.tsv" >>  "${_dragenProjectDir}/${_dragenProject}.Dragen.csv"
		else
			log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Project ${_dragenProject} is not a sWGS or WGS project, is there something wrong?"
		fi
	fi
		
		log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Done making the run_data_info and table file for project ${_dragenProject}"
		updateOrCreateDatabase "dragen${_dataType}" "${_dragenProjectDir}/${_dragenProject}.Dragen.csv" "${_dragenProjectDir}/${_dragenProject}.Dragen_runinfo.csv" "dragen${_dataType}" || return 1
}

function generateReports() {
	# shellcheck disable=SC1091
	source "${CHRONQC_TEMPLATE_DIRS}/reports.sh" || { 
		log4Bash 'ERROR' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "Failed to create all reports from the Chronqc database."  
	}
}

#
##
### Main.
##
#
#
# Get commandline arguments.
#

log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Parsing commandline arguments ..."
declare group=''
declare cli_datatype=""

while getopts ":g:d:l:h" opt
do
	case "${opt}" in
		h)
			showHelp
			;;
		g)
			group="${OPTARG}"
			;;
		d)
			cli_datatype="${OPTARG}"
			;;
		l)
			l4b_log_level="${OPTARG^^}"
			l4b_log_level_prio="${l4b_log_levels["${l4b_log_level}"]}"
			;;
		\?)
			log4Bash 'FATAL' "${LINENO}" "${FUNCNAME[0]:-main}" '1' "Invalid option -${OPTARG}. Try $(basename "${0}") -h for help."
			;;
		:)
			log4Bash 'FATAL' "${LINENO}" "${FUNCNAME[0]:-main}" '1' "Option -${OPTARG} requires an argument. Try $(basename "${0}") -h for help."
			;;
		*)
			log4Bash 'FATAL' "${LINENO}" "${FUNCNAME[0]:-main}" '1' "Unhandled option. Try $(basename "${0}") -h for help."
			;;
	esac
done

#
# Check commandline options.
#
if [[ -z "${group:-}" ]]
then
	log4Bash 'FATAL' "${LINENO}" "${FUNCNAME:-main}" '1' 'Must specify a group with -g.'
fi

#
# Source config files.
#
log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Sourcing config files ..."
declare -a configFiles=(
	"${CFG_DIR}/${group}.cfg"
	"${CFG_DIR}/${HOSTNAME_SHORT}.cfg"
	"${CFG_DIR}/sharedConfig.cfg"
)
for configFile in "${configFiles[@]}"
do
	if [[ -f "${configFile}" && -r "${configFile}" ]]
	then
		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "Sourcing config file ${configFile} ..."
		#
		# In some Bash versions the source command does not work properly with process substitution.
		# Therefore we source a first time with process substitution for proper error handling
		# and a second time without just to make sure we can use the content from the sourced files.
		#
		# Disable shellcheck code syntax checking for config files.
		# shellcheck source=/dev/null
		mixed_stdouterr=$(source "${configFile}" 2>&1) || log4Bash 'FATAL' "${LINENO}" "${FUNCNAME[0]:-main}" "${?}" "Cannot source ${configFile}."
		# shellcheck source=/dev/null
		source "${configFile}"  # May seem redundant, but is a mandatory workaround for some Bash versions.
	else
		log4Bash 'FATAL' "${LINENO}" "${FUNCNAME[0]:-main}" '1' "Config file ${configFile} missing or not accessible."
	fi
done

#
# Write access to prm storage requires data manager account.
#
if [[ "${ROLE_USER}" != "${ATEAMBOTUSER}" ]]
then
	log4Bash 'FATAL' "${LINENO}" "${FUNCNAME:-main}" '1' "This script must be executed by user ${ATEAMBOTUSER}, but you are ${ROLE_USER} (${REAL_USER})."
fi

#
# Make sure only one copy of this script runs simultaneously
# per data collection we want to copy to prm -> one copy per group.
# Therefore locking must be done after
# * sourcing the file containing the lock function,
# * sourcing config files,
# * and parsing commandline arguments,
# but before doing the actual data trnasfers.
#

lockFile="${TMP_ROOT_DIR}/logs/${SCRIPT_NAME}.lock"
thereShallBeOnlyOne "${lockFile}"
log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Successfully got exclusive access to lock file ${lockFile} ..."
log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Log files will be written to ${TMP_ROOT_DIR}/logs ..."

module load "ChronQC/${CHRONQC_VERSION}"

tmp_trendanalyse_dir="${TMP_ROOT_DIR}/trendanalysis/"
logs_dir="${TMP_ROOT_DIR}/logs/trendanalysis/"
mkdir -p "${TMP_ROOT_DIR}/logs/trendanalysis/"
chronqc_tmp="${tmp_trendanalyse_dir}/tmp/"
CHRONQC_DATABASE_NAME="${tmp_trendanalyse_dir}/database/"
today=$(date '+%Y%m%d')

# Determine processing order
DATATYPE_ORDER=(rawdata projects RNAprojects darwin dragen openarray ogm reports)

# Mapping: dataType + functions + inputdir
declare -A DATA_HANDLERS=(
	[rawdata]=processRawdata
	[projects]=processProjects
	[RNAprojects]=processRnaProjects
	[darwin]=processDarwin
	[dragen]=processDragen
	[openarray]=processOpenArray
	[ogm]=processOGM
	[reports]=generateReports
)

# Overwrite config defined dataTypes if a list was provided via commandline option '-d type'
if [[ -n "${cli_datatype}" ]]; then
	declare -A ENABLED_TYPES=()
	IFS=',' read -ra types <<< "${cli_datatype}"

	for t in "${types[@]}"; do
		if [[ -z "${DATA_HANDLERS[${t}]:-}" ]]; then
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '1' "Unknown datatype '${t}'"
			exit 1
		fi
		ENABLED_TYPES["${t}"]=true
	done
fi

# loop over DATATYPE_ORDER, that need to be processed, and skip when false.
for type in "${DATATYPE_ORDER[@]}"; do
	if [[ "${ENABLED_TYPES[${type}]:-false}" == "true" ]]; then
		processData "${type}" "${DATA_HANDLERS[${type}]}" "${INPUTDIRS[${type}]}"
	else
		log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Skip ${type} (disabled)"
	fi
done

chronqc_tmp="${tmp_trendanalyse_dir}/tmp/"
log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "cleanup ${chronqc_tmp}* ..."
rm -rf "${chronqc_tmp:-missing}"/*

trap - EXIT
exit 0