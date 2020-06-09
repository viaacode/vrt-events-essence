#!/bin/bash
### ARGS: $1 env $2 app-name
### script waits for next rollout
set +x
declare -i cur_ver
declare -i new_ver

get_currentversion(){
   cur_ver=`oc rollout status deploymentconfig ${2}-${1}| awk '{print $3}'| sed 's/"//g'|rev|cut -d'-' -f1 |rev |tail -n1`|| echo error
   if echo $cur_ver | grep -Eq '^[+-]?[0-9]+$'
   then
      echo revision-nr $cur_ver
   else
      echo "!! got string  sleeping!"
      sleep 40
      cur_ver=`oc rollout status deploymentconfig ${2}-${1}| awk '{print $3}'| sed 's/"//g'|rev|cut -d'-' -f1 |rev| tail -n1` 
   fi
   new_ver="$((cur_ver + 1))" || exit 1
}

get_currentversion $1 $2
oc rollout latest dc/${2}-${1}

echo "${1} version to compare ${cur_ver}, new version ${new_ver}"

while true ; do
   if [ $new_ver -eq 1 ]  ;then
      echo "Maybe other rollout is going on sleeping.. "
      sleep 25
      get_currentversion $1 $2
      oc rollout latest dc/${2}-${1}
   fi
   oc rollout status deploymentconfig ${2}-${1} --revision=${new_ver}
   if [ $? -ne 0 ]; then
    sleep 40
    else echo "$1" ROLLED OUT && exit 0
   fi
done