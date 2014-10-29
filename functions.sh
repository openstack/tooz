function check_port(){
    netstat -an 2>/dev/null | grep -q "$1"
    return $?
}
