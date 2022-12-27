#! /bin/bash
# Script to Delete failed Job Seq Key folder in hdfs. Script accepts JobSeqKey and hdfs parent folder as input parameters.
# j: JobSeqKey
# p: HDFS Path
# help_msg : Usage Function
#set -x

#Variable initialization
logFileDir=""
logFile=""
lockDir=""

#function to log stdout and file
loggerOut() {
  while read text
  do
        logTime=`date "+%Y%m%d %H:%M:%S"`
        if [[ "${logFile}" == "" ]]
        then
           echo ""${logTime}" : "${text}""
        else
            if [[ ! -f ${logFile} ]]
            then
                touch ${logFile}
                if [[ "${?}" -ne 0 ]]
                then
                    echo "Cann't create log file. Because config file is not passed or invalid"${logFile}""
                    if [[ -e "${lockDir}" ]]
                    then
                        rm ${lockDir}
                    fi
                    exit 1
                fi
            fi
            echo ""${logTime}" : "${text}"" | tee -a ${logFile}
        fi
  done
}

#usage function
helpMsg() {
           local MESSAGE="${@}"
           echo "${MESSAGE}" | loggerOut
           echo "Usage: ${0} [-j JobSeqKey] [-p hdfs path][-c configFile ]" | loggerOut
           echo "-i: JobSeqKey" | loggerOut
	   echo "-h: hdfs path" | loggerOut
           echo "-c: config file for constant values" | loggerOut
           echo "Cann't create log file. Because config file is not passed or invalid "${logFile}"" | loggerOut
           if [[ -e "${lockDir}" ]]
           then
               rm ${lockDir}
           fi
           exit 1
          }

#Validate argument type
if [[ $OPTIND -eq 0 ]]; then
  helpMsg
fi

#Validate number of arguments to the script
if [[ ${#} -lt 6 ]]; then
  helpMsg
fi

#Validate input argument for empty value
if [[ -z "${1}" ]]; then
  helpMsg
fi

#set -C
#if echo "$$" > "${lockDir}"
#then
  while getopts 'j:p:c:' opt; do
        case $opt in
                j) jobSeqKey=$OPTARG
               ;;
	        p) hdfsPath=$OPTARG
		;;
                c) configFl=$OPTARG
               ;;
                ?) helpMsg
               ;;
        esac
    ValidateInputArg='true'
  done
#Validate input arguments
 if [[ ${ValidateInputArg} != 'true' ]]
  then
      helpMsg
  fi
#read configuration values from config file
if [[ -f "${configFl}" ]]
then
    source "${configFl}"
    logFileDir=""${logFilePath}"$(basename ${0} ".sh")/"
    logFile="${logFileDir}"`date "+%Y%m%d%H%M"`".log"
    lockDir=""${lockFilePath}""/"$(basename ${0} ".sh").lock"
else
    echo ""${configFl}" is not a valid config file" | loggerOut
    helpMsg
fi

#function to perform cleanup
cleaupFiles(){
                    #Validate hdfs path
		    hdfs dfs -test -d "${hdfsPath}""/""${jobSeqKey}"
		     if [[ "${?}" -eq 0 ]] 
		     then
		    	hdfs dfs -rm -r -skipTrash "${hdfsPath}""/""${jobSeqKey}" | loggerOut
			# hdfs dfs -ls "${hdfsPath}""/""${jobSeqKey}"
		     else
		       echo "HDFS Path doen't exist" | loggerOut
		       rm "${lockDir}"
		       exit 1
		    fi
}

set -C
if echo "$$" > "${lockDir}"
then
    # call cleanupFiles function
    cleaupFiles
    #remove lock file created to check if another instance of script is running
    rm "${lockDir}"
else
    echo "lock dir path is incorrect or no permissions or another instance of the script is running "|loggerOut
    exit 1
fi

