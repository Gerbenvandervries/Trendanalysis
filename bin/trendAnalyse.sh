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
		-d InputDataType dragen|projects|RNAprojects|ogm|darwin|openarray|rawdata|all
		Providing InputDataType to run only a specific data type or "all" to run all types.
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
### Data proccessed functions.
##
#

function updateOrCreateDatabase() {

	local _db_table="${1}" #SequenceRun
	local _tableFile="${2}" #"${chronqc_tmp}/${_rawdata}.SequenceRun.csv"
	local _runDateInfo="${3}" #"${chronqc_tmp}/${_rawdata}.SequenceRun_run_date_info.csv"
	local _dataLabel="${4}" #"${_sequencer}" 
	local _job_controle_line_base="${5}" #"${_rawdata_job_controle_line_base}"
  local _forceCreate="${6}"
	
    log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Update database for project ${_tableFile}"    
    if doesTableExist "${_db_table}" || [[ "${_forceCreate}" == "false" ]]; then

      # updates existing table with new rundata.
      chronqc database --update --db "${CHRONQC_DATABASE_NAME}/chronqc_db/chronqc.stats.sqlite" \
				"${_tableFile}" \
				--db-table "${_db_table}" \
				--run-date-info "${_runDateInfo}" \
				"${_dataLabel}" || {
					log4Bash 'ERROR' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "Failed to import ${_tableFile} with ${_dataLabel} stored to Chronqc database." 
					return
        }
    else

      log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Create database for project ${_tableFile}"
      # Created non existing table, and adds new rundata.
      chronqc database --create -f \
        -o "${CHRONQC_DATABASE_NAME}" \
        "${_tableFile}" \
        --run-date-info "${_runDateInfo}" \
        --db-table "${_db_table}" \
        "${_dataLabel}" -f || {
          log4Bash 'ERROR' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "Failed to create database and import ${_tableFile} with ${_dataLabel} stored to Chronqc database." 
          return
        }
      fi
      log4Bash 'INFO' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "${FUNCNAME[0]} ${_tableFile} with ${_dataLabel} was stored in Chronqc database."
      log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "The line ${_job_controle_line_base} added to process.dataToTrendanalysis.finished file."
}

processRawdata()    { log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "  -> rawdata verwerken: $1"; }
processProjects()   { log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "  -> project verwerken: $1"; }
processRnaProjects(){ log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "  -> RNA-project verwerken: $1"; }
processDarwin()     { log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "  -> darwin verwerken: $1"; }
processDragen()     { log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "  -> dragen verwerken: $1"; }
processOpenarray()  { log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "  -> openarray verwerken: $1"; }
processOgm()        { log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "  -> ogm verwerken: $1"; }

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
declare InputDataType='all'

while getopts ":g:l:h" opt
do
	case "${opt}" in
		h)
			showHelp
			;;
		g)
			group="${OPTARG}"
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
case "${InputDataType}" in 
		dragen|projects|RNAprojects|darwin|openarray|rawdata|ogm|all)
			;;
		*)
			log4Bash 'FATAL' "${LINENO}" "${FUNCNAME[0]:-main}" '1' "Unhandled option. Try $(basename "${0}") -h for help."
			;;
esac
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

# if [[ "${ROLE_USER}" != "${ATEAMBOTUSER}" ]]
# then
# 	log4Bash 'FATAL' "${LINENO}" "${FUNCNAME:-main}" '1' "This script must be executed by user ${ATEAMBOTUSER}, but you are ${ROLE_USER} (${REAL_USER})."
# fi

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

#
## Loops over all rawdata folders and checks if it is already in chronQC database. If not than call function 'processRawdataToDB "${rawdata}" to process this project.'
#

module load "ChronQC/${CHRONQC_VERSION}"

TMP_ROOT_DIR='/groups/umcg-atd/tmp02/projects/umcg-gvdvries/'

tmp_trendanalyse_dir="${TMP_ROOT_DIR}/trendanalysis/"
logs_dir="${TMP_ROOT_DIR}/logs/trendanalysis/"
mkdir -p "${TMP_ROOT_DIR}/logs/trendanalysis/"
chronqc_tmp="${tmp_trendanalyse_dir}/tmp/"
CHRONQC_DATABASE_NAME="${tmp_trendanalyse_dir}/database/"
today=$(date '+%Y%m%d')

 

# Mapping: data Type + functions + inputdir
declare -A DATA_HANDLERS=(
  [rawdata]=processRawdata
  [projects]=processProjects
  [RNAprojects]=processRnaProjects
  [darwin]=processDarwin
  [dragen]=processDragen
  [openarray]=processOpenArray
  [ogm]=processOgm
)

# loop over data types from config that need to be processed, and skip when false.
for type in "${!DATA_HANDLERS[@]}"; do
  if [[ "${ENABLED_TYPES[$type]:-false}" == "true" ]]; then
    processData "$type" "${DATA_HANDLERS[$type]}" "${INPUTDIRS[$type]}"
  else
    log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Skip $type (disabled)"
  fi
done


trap - EXIT
exit 0
