#!/bin/bash -e
#---------------------- Modification History ----------------------------------------
# 01|venkat.V.S.|12-dec-2012| created
#
#
#
#
#
#---------------------------
#     Description 
#----------------------------
# Hadoop works best with big files. So this script will help us to group all small files into big files. 
# It also gives us an option to group files based on the max size.
#
#---------------------------
#        Author
#----------------------------
# Venkat.V.S.
# http://venkat-echo.blogspot.com
# https://github.com/venkat-vs-id
# 
#
#------------------------------
#       Disclaimer
#------------------------------
#THESE PROGRAMS (CODE or SOFTWARE or DOCUMENTATION) SHARED BY ME ARE FURNISHED "AS IS". 
#THESE PROGRAMS ARE NOT THROUGHLY TESTED UNDER ALL CONDITIONS. I, THEREFORE CANNOT GAURANTEE OR IMPY RELIABILITY, SERVICEABILITY 
#OR THE FUNCTION OF THESE PROGRAMS.I MAKE NO WARRANTY, EXPRESS OR IMPLIED, AS TO THE USEFULNESS OF THESE PROGRAMS FOR ANY PURPOSE. 
#I ASSUME NO RESPONSIBILITY FOR THE USE OF THESE PROGRAMS; OR TO PROVIDE TECHNICAL SUPPORT TO USERS.
#
#----------------------------------------------------------------------------------------
set -e;
trap 'echo "error"' ERR;
set -o pipefail;

declare underline=`tput smul`
declare nounderline=`tput rmul`
declare bold=`tput bold`
declare normal=`tput sgr0`
declare lMaxFileSize=0
declare -i lLookIntoSubFolders=0
declare lFilePath=""
declare lOutFilePath=""
declare lFileName=""
declare -i lFileCounter=0

echo -ne "\n"

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function usage()
{
echo -ne "\n"
echo -ne "${bold}********  Hadoop file Combiner ********************************************************************************************************************${normal}\n"

echo -ne "hadoop_file_combiner.sh -size -path [-name] [-opath] [-a] [-h] \n"

echo -ne "\t-size  ${underline}size_in_MB${nounderline} \n"
echo -ne "\t        Mandatory.\n"
echo -ne "\t         Small files are grouped together to a one or more big file(s). Each grouped file will not exceed the max size passed on in this argument.\n"
echo -ne "\t          Grouped files are named like bigfile1, bigfile2 and so on.\n"
echo -ne "\t          If no size is given then all files are grouped into one big Single File.\n"

echo -ne "\t-path  ${underline}parent dir${nounderline} \n"
echo -ne "\t          Mandatory.\n"
echo -ne "\t           Path of the folder where all the small files are available. By default only files which are directly under this folder will be grouped.\n"
echo -ne "\t           If you want to pick the files from the sub-folder too then use -a or --all argument.\n"
echo -ne "\t           If you are having WILDCARDS in your path arguments,then please eclose the values in double quotes(\") to avoid command-line globing.\n"

echo -ne "\t-name ${underline}filename with wildcards${nounderline} \n"
echo -ne "\t	     This argument helps to select files with specific name patterns.

echo -ne "\t-opath ${underline}out file dir path${nounderline} \n"
echo -ne "\t	     This is the dir path to which the output file is written. 

echo -ne "\t-a | --all \n"   
echo -ne "\t         If you would like to pick all the files under the given -path,including the files under the sub-folder,then use this paramater.\n"

echo -ne "\t-h | --help \n"   

echo -ne "${bold}***************************************************************************************************************************************************${normal}\n\n"

exit 1;  
}
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function error_n_exit()
{
  echo -ne "$1 \n\n" 1>&2; 
  exit 1;
}
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function validate_parameters()
{
  declare -i l_res=0;

  if [ -z "${lMaxFileSize}" ]; then

     error_n_exit "ERROR: -size is a mandatory parameter and can not be EMPTY. for help --help.";
  fi

  if [ -z "${lFilePath}" ]; then

     error_n_exit "ERROR: -path is a mandatory parameter and can not be EMPTY. for help --help.";
  fi

  if [ -z "${lOutFilePath}" ]; then
     lOutFilePath=${lFilePath};
  fi

  
  #-- use find to get the count of dir 
  l_res=$(find ${lOutFilePath} -maxdepth 0 -type d | wc -l );

  if [ "${l_res}" -ne "1" ]; then
     
     error_n_exit "Please provide a valid outpath. Output directory has to be a valid directory and pointing to a single directory";
  fi 
 
  if [[ -n ${lOutFilePath} && ! -w ${lOutFilePath} ]]; then
     
     error_n_exit "Do not have write permission on the oPath directory - ${lOutFilePath}";
  fi 

  if [ -z ${lFileName} ]; then
     lFileName="*";
  fi
 
  echo -ne "rarameters -> lMaxFileSize           = ${lMaxFileSize} MB\n"
  echo -ne "Parameters -> lLookIntoSubFolders = ${lLookIntoSubFolders}\n"
  echo -ne "Parameters -> lFilePath           = ${lFilePath}\n"
  echo -ne "Parameters -> lFileName           = ${lFileName}\n"
  echo -ne "Parameters -> lOutFilePath        = ${lOutFilePath}\n"

  echo -ne "\n"
}
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function combine_files()
{
  declare -i k=0;
  declare    kSize=0;
  declare    l_outFileName="";
  declare    l_fileName=""
  declare    l_fileSize=0;
  declare -a l_arrFiles;
  declare -i l_depth=9999;
 
  if [ "${lLookIntoSubFolders}" -ne 1 ]; then  #-- i.e if -all is not used then we check only root folder
      l_depth=1;
  fi

  for ifx in $(find ${lFilePath} -maxdepth ${l_depth}  -type f -name "${lFileName}"  -printf "%p:%s\n" )
  do
      if [ "$?" != "0" ]; then
         error_n_exit "Error while trying to read file/folder";
      fi
      l_fileName=`echo ${ifx} | cut -d: -f1`;
      l_fileSize=`echo ${ifx} | cut -d: -f2`; 	      

      l_fileSize=$(echo "scale=2; $l_fileSize/1024/1024"|bc );
 

      if [ "$(echo "${l_fileSize}>${lMaxFileSize}"|bc)" -eq "1" ]; then
         error_n_exit "Error:${l_fileName} - Size of this file(${l_fileSize}) is greater than the -size argument(${lMaxFileSize}MB). -size argument values is the max possible size for the combined file(s)."   
      fi
      
      k=$k+1;
      l_arrFiles[k]=${ifx};	   
  done

  #----------- If it has come to this spot then it means the we can combine the files --------------
  k=0;
  kSize=0;
  l_outFileName="tmp";
  for jfx in "${l_arrFiles[@]}"  
  do

     l_fileName=`echo ${jfx} | cut -d: -f1`;
     l_fileSize=`echo ${jfx} | cut -d: -f2`;
     
     l_fileSize=$(echo "scale=2; ${l_fileSize}/1024/1024"|bc ); # -- size in MB
     
     if [ "${l_outFileName}" != "tmp" ]; then
   
        kSize=$(stat -c %s ${l_outFileName});
        kSize=$(echo "scale=2; ${kSize}/1024/1024"|bc ); # -- size in MB
     fi	 
     kSize=$(echo ${l_fileSize}+${kSize}+1|bc);  # -- +1 MB is for safety      
    
     if [[ "$(echo "${kSize}>${lMaxFileSize}"|bc)" -eq "1" || ${k} -eq 0 ]]; then
 
        k=$k+1;
        kSize=0;
        l_outFileName=`echo "${lOutFilePath}/thebigfile_${k}.txt"`;
    
	if [ -e ${l_outFileName} ]; then
	   rm ${l_outFileName};
	   if [ "$?" -ne "0" ]; then
 	      error_n_exit " Unable to delete the file - ${l_outFileName}";
	   else
  	      echo -ne " File already exists, so it was deleted - ${l_outFileName} \n";
	   fi		
	fi
	touch ${l_outFileName};
        echo -ne "-----out file>>>${l_outFileName}\n"     
     fi

     cat ${l_fileName} >> ${l_outFileName};
  done 
  
  exit 0; 
}
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

if [ $# -eq 0 ]; then
  usage;
else

  while [ -n "$1" ]; do
            
      echo -ne "Processing paramer ==> $1 \n";
      case $1 in
        -path      ) shift;
		     lFilePath=$1 
                     if [[ ( -n "$2" ) && ( -f $2 ) ]]; then
			error_n_exit "ERROR: Please use double quotes(\") if you are using wildcards in your -path arguments. Command-Line globing is not supported.";
		     fi
		     ;;
        -a|--all  )  lLookIntoSubFolders=1;
                     ;;
        -size     )  lMaxFileSize=$2; shift;
                     ;;
        -h|--help )  usage
                     ;;	 
	-name	  ) shift
		    lFileName=$1;
		    ;;
	-opath	 )  shift
		    lOutFilePath=$1;
		    ;; 	      
        *) error_n_exit "ERROR: Wrong Parameter usage";
     esac

     shift
  done
fi

validate_parameters;  

combine_files;

exit 0;

