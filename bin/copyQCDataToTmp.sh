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
### Functions.
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

function copyQCdataToTmp() {

	local _data="${1}" #rawdata
	local _log_controle_file_base="${2}"
	local _log_line_base="${3}"
	local _prm_qc_dir="${4}" #"${_prm_rawdata_dir}/${_rawdata}/Info/SequenceRun"*
	local _tmp_qc_dir="${5}" #${TMP_ROOT_DIR}/trendanalysis/rawdata/${_rawdata}/

	log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "Data ${_data} is not yet copied to tmp, start rsyncing.."
	echo "${_log_line_base}.started" >> "${_log_controle_file_base}"
	log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "${_data} found on ${_prm_qc_dir}, start rsyncing.."
	rsync -av --rsync-path="sudo -u ${group}-ateambot rsync" "${_prm_qc_dir}" "${DESTINATION_DIAGNOSTICS_CLUSTER}:${_tmp_qc_dir}" \
	|| {
		log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "Failed to rsync ${_data}"
		log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    from ${_prm_qc_dir}"
		log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    to ${DESTINATION_DIAGNOSTICS_CLUSTER}:${_tmp_qc_dir}/"
		echo "${_log_line_base}.failed" >> "${_log_controle_file_base}"
		return
		}
	sed "/${_log_line_base}.failed/d" "${_log_controle_file_base}" > "${_log_controle_file_base}.tmp"
	sed "/${_log_line_base}.started/d" "${_log_controle_file_base}.tmp" > "${_log_controle_file_base}.tmp2"
	echo "${_log_line_base}.finished" >> "${_log_controle_file_base}.tmp2"
	sync
	mv "${_log_controle_file_base}.tmp2" "${_log_controle_file_base}"
	log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Finished copying data: ${_data}"
}

function copyQCrawdataToTmp() {

	local _data="${1}" #rawdata
	local _log_controle_file_base="${2}"
	local _log_line_base="${3}"
	local _prm_qc_dir="${4}" #"${_prm_rawdata_dir}/${_rawdata}/Info/"
	local _tmp_qc_dir="${5}" #${TMP_ROOT_DIR}/trendanalysis/rawdata/${_rawdata}/

	log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "Data ${_data} is not yet copied to tmp, start rsyncing.."
	echo "${_log_line_base}.started" >> "${_log_controle_file_base}"
	log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "${_data} found on ${_prm_qc_dir}, start rsyncing.."
	rsync -av --rsync-path="sudo -u ${group}-ateambot rsync" "${_prm_qc_dir}/SequenceRun"* "${DESTINATION_DIAGNOSTICS_CLUSTER}:${_tmp_qc_dir}" \
	|| {
		log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "Failed to rsync ${_data}"
		log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    from ${_prm_qc_dir}"
		log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    to ${DESTINATION_DIAGNOSTICS_CLUSTER}:${_tmp_qc_dir}/"
		echo "${_log_line_base}.failed" >> "${_log_controle_file_base}"
		return
		}
	sed "/${_log_line_base}.failed/d" "${_log_controle_file_base}" > "${_log_controle_file_base}.tmp"
	sed "/${_log_line_base}.started/d" "${_log_controle_file_base}.tmp" > "${_log_controle_file_base}.tmp2"
	echo "${_log_line_base}.finished" >> "${_log_controle_file_base}.tmp2"
	sync
	mv "${_log_controle_file_base}.tmp2" "${_log_controle_file_base}"
	log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Finished copying data: ${_data}"
}

function copyQCdarwindataToTmp() {

	local _data="${1}" #rawdata
	local _log_controle_file_base="${2}"
	local _log_line_base="${3}"
	local _prm_qc_dir="${4}" #"${_prm_rawdata_dir}/${_rawdata}/Info/"
	local _fileType="${5}"
	local _fileDate="${6}"
	local _tmp_qc_dir="${7}" #${TMP_ROOT_DIR}/trendanalysis/rawdata/${_rawdata}/

	log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "Data ${_data} is not yet copied to tmp, start rsyncing.."
	echo "${_log_line_base}.started" >> "${_log_controle_file_base}"
	log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "${_data} found on ${_prm_qc_dir}, start rsyncing.."
	rsync -av --rsync-path="sudo -u ${group}-ateambot rsync" "${_prm_qc_dir}/${fileType}"*"${fileDate}.csv" "${DESTINATION_DIAGNOSTICS_CLUSTER}:${_tmp_qc_dir}" \
	|| {
		log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "Failed to rsync ${_data}"
		log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    from ${_prm_qc_dir}"
		log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    to ${DESTINATION_DIAGNOSTICS_CLUSTER}:${_tmp_qc_dir}/"
		echo "${_log_line_base}.failed" >> "${_log_controle_file_base}"
		return
		}
	sed "/${_log_line_base}.failed/d" "${_log_controle_file_base}" > "${_log_controle_file_base}.tmp"
	sed "/${_log_line_base}.started/d" "${_log_controle_file_base}.tmp" > "${_log_controle_file_base}.tmp2"
	echo "${_log_line_base}.finished" >> "${_log_controle_file_base}.tmp2"
	sync
	mv "${_log_controle_file_base}.tmp2" "${_log_controle_file_base}"
	log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Finished copying data: ${_data}"
}

function copyQCProjectdataToTmp() {

	local _project="${1}"
	local _project_job_controle_file_base="${2}"
	local _line_base="${3}"
	local _prm_project_dir="${4}" #"/groups/${group}/${prm_dir}/projects/"
	
	log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "Working on ${_prm_project_dir}/${_project}"
	
	# The RNA projects will be copied to ${TMP_ROOT_DIR}/trendanalysis/RNAprojects/
	if [[ -e "${_prm_project_dir}/${_project}/run01/results/multiqc_data/${_project}.run_date_info.csv" && "${_project}" =~ "RNA" ]]
	then
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Starting on ${_project}"
		echo "${_line_base}.started" >> "${_project_job_controle_file_base}"
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "${_project} found on ${_prm_project_dir}, start rsyncing.."
		rsync -av --rsync-path="sudo -u ${group}-ateambot rsync" "${_prm_project_dir}/${_project}/run01/results/multiqc_data/"* "${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/RNAprojects/${_project}/" \
		|| {
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "Failed to rsync ${_project}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    from ${_prm_project_dir}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    to ${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/RNAprojects/${_project}/"
			echo "${_line_base}.failed" >> "${_project_job_controle_file_base}"
			return
			}
		rsync -av --rsync-path="sudo -u ${group}-ateambot rsync" "${_prm_project_dir}/${_project}/run01/results/${_project}.csv" "${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/RNAprojects/${_project}/" \
		|| {
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "Failed to rsync ${_project}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    from ${_prm_project_dir}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    to ${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/RNAprojects/${_project}/"
			echo "${_line_base}.failed" >> "${_project_job_controle_file_base}"
			return
			}
		sed "/${_line_base}.failed/d" "${_project_job_controle_file_base}" > "${_project_job_controle_file_base}.tmp"
		sed "/${_line_base}.started/d" "${_project_job_controle_file_base}.tmp" > "${_project_job_controle_file_base}.tmp2"
		echo "${_line_base}.finished" >> "${_project_job_controle_file_base}.tmp2"
		sync
		mv "${_project_job_controle_file_base}.tmp2" "${_project_job_controle_file_base}"
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Finished copying data: ${_project}"

	# The inhouse projects (Exoom, targeted) will be copied to ${TMP_ROOT_DIR}/trendanalysis/projects/
	elif [[ -e "${_prm_project_dir}/${_project}/run01/results/multiqc_data/${_project}.run_date_info.csv" ]]
	then
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Starting on ${_project}"
		echo "${_line_base}.started" >> "${_project_job_controle_file_base}"
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "${_project} found on ${_prm_project_dir}, start rsyncing.."
		rsync -av --rsync-path="sudo -u ${group}-ateambot rsync" "${_prm_project_dir}/${_project}/run01/results/multiqc_data/"* "${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/projects/${_project}/" \
		|| {
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "Failed to rsync ${_project}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    from ${_prm_project_dir}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    to ${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/projects/${_project}/"
			echo "${_line_base}.failed" >> "${_project_job_controle_file_base}"
			return
			}
		rsync -av --rsync-path="sudo -u ${group}-ateambot rsync" "${_prm_project_dir}/${_project}/run01/results/${_project}.csv" "${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/projects/${_project}/" \
		|| {
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "Failed to rsync ${_project}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    from ${_prm_project_dir}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    to ${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/projects/${_project}/"
			echo "${_line_base}.failed" >> "${_project_job_controle_file_base}"
			return
			}
		sed "/${_line_base}.failed/d" "${_project_job_controle_file_base}" > "${_project_job_controle_file_base}.tmp"
		sed "/${_line_base}.started/d" "${_project_job_controle_file_base}.tmp" > "${_project_job_controle_file_base}.tmp2"
		echo "${_line_base}.finished" >> "${_project_job_controle_file_base}.tmp2"
		sync
		mv "${_project_job_controle_file_base}.tmp2" "${_project_job_controle_file_base}"
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Finished copying data: ${_project}"
		# The Dragen project (Exoom, WGS, sWGS) wil be copied to ${TMP_ROOT_DIR}/trendanalysis/dragen/
	elif  [[ -e "${_prm_project_dir}/${_project}/run01/results/qc/statistics/${_project}.Dragen_runinfo.csv" && "${_project}" =~ "Exoom" ]]
	then
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Processing Exoom ${_project} ..."
		echo "${_line_base}.started" >> "${_project_job_controle_file_base}"
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "${_project} found on ${_prm_project_dir}, start rsyncing.."
		# shellcheck disable=SC2029
		ssh "${group}-ateambot@${DESTINATION_DIAGNOSTICS_CLUSTER}" mkdir -p "${TMP_ROOT_DIR}/trendanalysis/dragen/${_project}/"
		rsync -av --rsync-path="sudo -u ${group}-ateambot rsync" "${_prm_project_dir}/${_project}/run01/results/qc/statistics/"* "${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/dragen/${_project}/" \
		|| {
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "Failed to rsync ${_project}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    from ${_prm_project_dir}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    to ${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/dragen/${_project}/"
			echo "${_line_base}.failed" >> "${_project_job_controle_file_base}"
			return
			}
		rsync -av --rsync-path="sudo -u ${group}-ateambot rsync" "${_prm_project_dir}/${_project}/run01/results/${_project}.csv" "${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/dragen/${_project}/" \
		|| {
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "Failed to rsync ${_project}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    from ${_prm_project_dir}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    to ${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/dragen/${_project}/"
			echo "${_line_base}.failed" >> "${_project_job_controle_file_base}"
			return
			}
		sed "/${_line_base}.failed/d" "${_project_job_controle_file_base}" > "${_project_job_controle_file_base}.tmp"
		sed "/${_line_base}.started/d" "${_project_job_controle_file_base}.tmp" > "${_project_job_controle_file_base}.tmp2"
		echo "${_line_base}.finished" >> "${_project_job_controle_file_base}.tmp2"
		sync
		mv "${_project_job_controle_file_base}.tmp2" "${_project_job_controle_file_base}"
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Finished copying data: ${_project}" 
	elif [[ -e "${_prm_project_dir}/${_project}/run01/results/qc/stats.tsv" && "${_project}" =~ "WGS" ]]
	then
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Processing WGS ${_project} ..."
		echo "${_line_base}.started" >> "${_project_job_controle_file_base}"
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "${_project} found on ${_prm_project_dir}, start rsyncing.."
		# shellcheck disable=SC2029
		ssh "${group}-ateambot@${DESTINATION_DIAGNOSTICS_CLUSTER}" mkdir -p "${TMP_ROOT_DIR}/trendanalysis/dragen/${_project}/"
		rsync -av --rsync-path="sudo -u ${group}-ateambot rsync" "${_prm_project_dir}/${_project}/run01/results/qc/stats.tsv" "${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/dragen/${_project}/${_project}.stats.tsv" \
		|| {
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "Failed to rsync ${_project}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    from ${_prm_project_dir}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    to ${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/dragen/${_project}/"
			echo "${_line_base}.failed" >> "${_project_job_controle_file_base}"
			return
			}
		rsync -av --rsync-path="sudo -u ${group}-ateambot rsync" "${_prm_project_dir}/${_project}/run01/results/${_project}.csv" "${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/dragen/${_project}/" \
		|| {
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "Failed to rsync ${_project}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    from ${_prm_project_dir}"
			log4Bash 'ERROR' "${LINENO}" "${FUNCNAME:-main}" '0' "    to ${DESTINATION_DIAGNOSTICS_CLUSTER}:${TMP_ROOT_DIR}/trendanalysis/dragen/${_project}/"
			echo "${_line_base}.failed" >> "${_project_job_controle_file_base}"
			return
			}
		sed "/${_line_base}.failed/d" "${_project_job_controle_file_base}" > "${_project_job_controle_file_base}.tmp"
		sed "/${_line_base}.started/d" "${_project_job_controle_file_base}.tmp" > "${_project_job_controle_file_base}.tmp2"
		echo "${_line_base}.finished" >> "${_project_job_controle_file_base}.tmp2"
		sync
		mv "${_project_job_controle_file_base}.tmp2" "${_project_job_controle_file_base}"
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Finished copying data for ${_project}." 
	else
		log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "There is no QC data for project ${_project}; nothing to rsync."
	fi

}

function showHelp() {
	#
	# Display commandline help on STDOUT.
	#
	cat <<EOH
===============================================================================================================
Script to copy (rsync) QC data from prm to tmp.
NGS project MultiQC data, sequencerun information from rawdata and everything Adlas/Darwin can produce.

Usage:

	$(basename "${0}") OPTIONS

Options:

	-h	Show this help.
	-g	[group]
		Group for which to process data.
	-d	inputDataType dragen|projects|RNAprojects|ogm|darwin|openarray|rawdata|all
		Providing InputDataType to run only a specific data type or "all" to run all types.
	-l	[level]
		Log level.
		Must be one of TRACE, DEBUG, INFO (default), WARN, ERROR or FATAL.

Config and dependencies:

	This script needs 4 config files, which must be located in ${CFG_DIR}:
		1. <group>.cfg       for the group specified with -g
		2. <this_host>.cfg   for this server. E.g.: "${HOSTNAME_SHORT}.cfg"
		3. <source_host>.cfg for the source server. E.g.: "<hostname>.cfg" (Short name without domain)
		4. sharedConfig.cfg  for all groups and all servers.
	In addition the library sharedFunctions.bash is required and this one must be located in ${LIB_DIR}.
===============================================================================================================
EOH
	trap - EXIT
	exit 0
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
declare InputDataType='all'
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
			InputDataType="${OPTARG}"
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
	"${HOME}/molgenis.cfg"
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

if [[ "${ROLE_USER}" != "${ATEAMBOTUSER}" ]]
then
	log4Bash 'FATAL' "${LINENO}" "${FUNCNAME:-main}" '1' "This script must be executed by user ${ATEAMBOTUSER}, but you are ${ROLE_USER} (${REAL_USER})."
fi

infoServerLocation="${HOSTNAME_PRM}"
infoLocation="/groups/${group}/${PRM_LFS}/trendanalysis/"
hashedSource="$(printf '%s:%s' "${infoServerLocation}" "${infoLocation}" | md5sum | awk '{print $1}')"
lockFile="/groups/${group}/${DAT_LFS}/trendanalysis/logs/${SCRIPT_NAME}_${hashedSource}.lock"
thereShallBeOnlyOne "${lockFile}"
log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Successfully got exclusive access to lock file ${lockFile} ..."
log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Log files will be written to ${PRM_ROOT_DIR}/trendanalysis/logs/ ..."

#
## Loops through all rawdata folders and checks if the QC data  is already copied to tmp. If not than call function copyQCRawdataToTmp
#
log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "starting checking the prm's for raw QC data"
mkdir -p "${DAT_ROOT_DIR}/trendanalysis/logs/"
datLogsDir="${DAT_ROOT_DIR}/trendanalysis/logs/"

if [[ "${InputDataType}" == "all" ]] || [[ "${InputDataType}" == "rawdata" ]]; then
	for prm_dir in "${ALL_PRM[@]}"
	do
		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "looping through ${prm_dir}"
		readarray -t rawdataArray < <(find "/groups/${group}/${prm_dir}/rawdata/ngs/" -maxdepth 1 -mindepth 1 -type d -name "[!.]*" | sed -e "s|^/groups/${group}/${prm_dir}/rawdata/ngs/||")
	
		if [[ "${#rawdataArray[@]}" -eq '0' ]]
		then
			log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "No rawdata found @ /groups/${group}/${prm_dir}/rawdata/ngs/."
		else
			for rawdata in "${rawdataArray[@]}"
			do
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Processing rawdata ${rawdata} ..."
				rawdata_job_controle_file_base="${datLogsDir}/${prm_dir}.${SCRIPT_NAME}.rawdata"
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Creating logs file: ${rawdata_job_controle_file_base}"
				rawdata_job_controle_line_base="${rawdata}.${SCRIPT_NAME}"
				prm_rawdata_dir="/groups/${group}/${prm_dir}/rawdata/ngs/"
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "checking: ${prm_rawdata_dir}"
				touch "${rawdata_job_controle_file_base}"
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "if grep -Fxq ${rawdata_job_controle_line_base}.finished ${rawdata_job_controle_file_base}"
				if grep -Fxq "${rawdata_job_controle_line_base}.finished" "${rawdata_job_controle_file_base}"
				then
					log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Skipping already processed batch ${rawdata}."
					continue
				else
					log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "starting function copyQCdataToTmp for rawdata ${rawdata}."
					if [[ -e "${prm_rawdata_dir}/${rawdata}/Info/SequenceRun.csv" ]]
					then
						log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "Sequencerun ${rawdata} is not yet copied to tmp, start rsyncing.."
						copyQCrawdataToTmp "${rawdata}" "${rawdata_job_controle_file_base}" "${rawdata_job_controle_line_base}" "${prm_rawdata_dir}/${rawdata}/Info/" "${TMP_ROOT_DIR}/trendanalysis/rawdata/${rawdata}/"
					else
						log4Bash 'TRACE' "${LINENO}" "${FUNCNAME[0]:-main}" '0' "There are no QC files for sequencerun ${rawdata}, skipping.."
					fi
				fi
			done
			rm -vf "${rawdata_job_controle_file_base}.tmp"
		fi
	done
fi

# Loops through all project data folders and checks if the QC data  is already copied to tmp. If not than call function copyQCProjectdataToTmp
if [[ "${InputDataType}" == "all" ]] || [[ "${InputDataType}" == "projects" ]]; then
	log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "starting checking the prm's for project QC data"
	for prm_dir in "${ALL_PRM[@]}"
	do
		log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "looping through ${prm_dir}"
		readarray -t projectdata < <(find "/groups/${group}/${prm_dir}/projects/" -maxdepth 1 -mindepth 1 -type d -name "[!.]*" | sed -e "s|^/groups/${group}/${prm_dir}/projects/||")
		if [[ "${#projectdata[@]}" -eq '0' ]]
		then
			log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "No projectdata found @ ${PRM_ROOT_DIR}/projects/."
		else
			for project in "${projectdata[@]}"
			do
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "Processing project ${project} ..."
				project_job_controle_file_base="${datLogsDir}/${prm_dir}.${SCRIPT_NAME}.projects"
				project_job_controle_line_base="${project}.${SCRIPT_NAME}"
				touch "${project_job_controle_file_base}"
				log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Processing run ${project} ..."
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "project_job_controle_file_base= ${project_job_controle_file_base}"
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "project_job_controle_line_base= ${project_job_controle_line_base}"
				log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "if grep -Fxq \"${project_job_controle_line_base}.finished\" \"${project_job_controle_file_base}\""
				if grep -Fxq "${project_job_controle_line_base}.finished" "${project_job_controle_file_base}"
				then
					log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "Skipping already processed batch ${project}."
					continue
				else
					log4Bash 'INFO' "${LINENO}" "${FUNCNAME:-main}" '0' "no ${project_job_controle_line_base}.finished present, in file ${project_job_controle_file_base}, checking QC data for project ${project}."
					copyQCProjectdataToTmp "${project}" "${project_job_controle_file_base}" "${project_job_controle_line_base}" "/groups/${group}/${prm_dir}/projects/"
				fi
			done
			rm -vf "${project_job_controle_file_base}.tmp"
		fi
	done
fi

#
## check if darwin left any new files for us on dat05 to copy to tmp05
#
if [[ "${InputDataType}" == "all" ]] || [[ "${InputDataType}" == "darwin" ]]; then
	for dat_dir in "${ALL_DAT[@]}"
	do
		import_dir="/groups/${group}/${dat_dir}/trendanalysis/"
	
		readarray -t darwindata < <(find "${import_dir}/" -maxdepth 1 -mindepth 1 -type f -name "*runinfo*" | sed -e "s|^${import_dir}/||")
		
		if [[ "${#darwindata[@]}" -eq '0' ]]
		then
			log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "no new darwin files present in ${import_dir}"
		else
			for darwinfile in "${darwindata[@]}"
			do
				log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Start processing ${darwinfile}"
				runinfoFile=$(basename "${darwinfile}" .csv)
				fileType=$(cut -d '_' -f1 <<< "${runinfoFile}")
				fileDate=$(cut -d '_' -f3 <<< "${runinfoFile}")
				tableFile="${fileType}_${fileDate}.csv"
				runinfoCSV="${runinfoFile}.csv"
				darwin_job_controle_file_base="${datLogsDir}/${dat_dir}.${SCRIPT_NAME}.darwin"
				darwin_job_controle_line_base="${fileType}-${fileDate}.${SCRIPT_NAME}"
				if grep -Fxq "${darwin_job_controle_line_base}.finished" "${darwin_job_controle_file_base}"
				then
					log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "${darwin_job_controle_line_base}.finished present"
					log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "${runinfoFile} data is already processed, but there is new data on ${dat_dir}, check if previous rsync went okay"
				else
					log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "no ${darwin_job_controle_line_base}.finished present, starting rsyncing ${tableFile} and ${runinfoCSV}"
					copyQCdarwindataToTmp "${runinfoFile}" "${darwin_job_controle_file_base}" "${darwin_job_controle_line_base}" "${import_dir}" "${fileType}" "${fileDate}" "${TMP_ROOT_DIR}/trendanalysis/darwin/"
					if grep -Fxq "${darwin_job_controle_line_base}.finished" "${darwin_job_controle_file_base}"
					then
						log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "rsyncing ${tableFile} and ${runinfoCSV} done, moving them to /archive/"
						mv "${import_dir}/${tableFile}" "${import_dir}/archive/"
						mv "${import_dir}/${runinfoCSV}" "${import_dir}/archive/"
					fi
				fi
			done
			rm -vf "${darwin_job_controle_file_base}.tmp"
		fi
	done
fi
#
## check the openarray folder for new data, /groups/umcg-gap/dat06/openarray/
#
if [[ "${InputDataType}" == "all" ]] || [[ "${InputDataType}" == "openarray" ]]; then
	for dat_dir in "${ALL_DAT[@]}"
	do
		import_dir_openarray="/groups/${OPARGROUP}/${dat_dir}/openarray/"
		readarray -t openarraydata < <(find "${import_dir_openarray}/" -maxdepth 1 -mindepth 1 -type d -name "[!.]*" | sed -e "s|^${import_dir_openarray}/||")
		if [[ "${#openarraydata[@]}" -eq '0' ]]
		then
			log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "no new openarray files present in ${import_dir_openarray}"
		else
			for openarraydir in "${openarraydata[@]}"
			do
				log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Start processing ${openarraydir}"
				QCFile=$(find "${import_dir_openarray}/${openarraydir}/" -maxdepth 1 -mindepth 1 -type f -name "*_QC_Summary.txt")
				if [[ -e "${QCFile}" ]]
				then 
					baseQCFile=$(basename "${QCFile}" .txt)
					openarray_job_controle_file_base="${datLogsDir}/${dat_dir}.${SCRIPT_NAME}.openarray"
					openarray_job_controle_line_base="${baseQCFile}_${SCRIPT_NAME}"
					if grep -Fxq "${openarray_job_controle_line_base}.finished" "${openarray_job_controle_file_base}"
					then
						log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "${openarray_job_controle_line_base}.finished present"
						log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "${QCFile} data is already processed"
					else
						log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "no ${openarray_job_controle_line_base}.finished present, starting rsyncing ${QCFile}."
						log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "baseQCFile=${baseQCFile}"
						log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "QCFile=${QCFile}"
						copyQCdataToTmp "${baseQCFile}" "${openarray_job_controle_file_base}" "${openarray_job_controle_line_base}" "${QCFile}" "${TMP_ROOT_DIR}/trendanalysis/openarray/${baseQCFile}/"
					fi
				else
					log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "QC file for project ${openarraydir} is not available"
				fi
			done
			rm -vf "${openarray_job_controle_file_base}.tmp"
		fi
	done
fi
#
## check the ogm folder for new data, /groups/umcg-ogm/prm67/
#
if [[ "${InputDataType}" == "all" ]] || [[ "${InputDataType}" == "ogm" ]]; then
	for dat_dir in "${ALL_DAT[@]}"
	do
		readarray -t ogmdata < <(find "${OGMTRENDANALYSIS}/" -maxdepth 1 -mindepth 1 -type f -name "[!.]*" | sed -e "s|^${OGMTRENDANALYSIS}/||")
		
		if [[ "${#ogmdata[@]}" -eq '0' ]]
		then
			log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "no new ogm files present in ${OGMTRENDANALYSIS}"
		else
			for ogmfile in "${ogmdata[@]}"
			do
				log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "Start processing ${ogmfile}"
				baseogmfile=$(basename "${ogmfile}" .csv)
				ogm_job_controle_file_base="${datLogsDir}/${OGMPRM}.${SCRIPT_NAME}.ogm"
				ogm_job_controle_line_base="${baseogmfile}_${SCRIPT_NAME}"
				if grep -Fxq "${ogm_job_controle_line_base}.finished" "${ogm_job_controle_file_base}"
				then
					log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "${ogm_job_controle_line_base}.finished present"
					log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "${baseogmfile} data is already processed"
				else
					log4Bash 'TRACE' "${LINENO}" "${FUNCNAME:-main}" '0' "no ${ogm_job_controle_line_base}.finished present, starting rsyncing ${baseogmfile}."
					log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "OGMTRENDANALYSIS=${OGMTRENDANALYSIS}"
					log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' "baseogmfile=${baseogmfile}"
					copyQCdataToTmp "${baseogmfile}" "${ogm_job_controle_file_base}" "${ogm_job_controle_line_base}" "${OGMTRENDANALYSIS}/${ogmfile}" "${TMP_ROOT_DIR}/trendanalysis/ogm/metricsInput/"
				fi
			done
			rm -vf "${ogm_job_controle_file_base}.tmp"
		fi
	done
fi

log4Bash 'DEBUG' "${LINENO}" "${FUNCNAME:-main}" '0' 'Finished!'

trap - EXIT
exit 0

