# Global log level
# 1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL
LOG_LEVEL=0

# Function to log messages with colored output
function log_message() {
    local msg_type=$1
    local message=$2
    local color
    local no_color="\033[0m" # Reset to default terminal color
    local msg_level

    # Check if only one argument is provided
    if [ $# -eq 1 ]; then
        message=$1
        color=$no_color  # Use default terminal color for unspecified message types
    else
        # Set color based on message type
        case $msg_type in
            DEBUG) color="\x1b[37m" ; msg_level=1 ;;                # Grey for DEBUG
            INFO) color="\033[0;32m" ; msg_level=2 ;;               # Green for INFO
            WARNING) color="\x1b[38;2;255;165;0m" ; msg_level=3 ;;  # Orange for WARNING
            ERROR) color="\033[0;31m" ; msg_level=4 ;;              # Red for ERROR
            CRITICAL) color="\x1b[31;1m" ; msg_level=5 ;;           # Bold red for CRITICAl
            *) color=$no_color ; msg_level=0 ;;                     # Default terminal color
        esac
    fi

    # If no message type was provided, shift arguments
    if [ $# -eq 1 ]; then
        message=$msg_type
        msg_type="INFO" # Default to INFO level for single-argument calls
        msg_level=2
    fi

    # Check if the message should be printed based on the log level
    if [ $msg_level -ge $LOG_LEVEL ]; then
        # If no message type was provided, shift arguments
        if [ $# -eq 1 ]; then
            message=$1
        fi
        echo -e "${color}$(date '+%Y-%m-%d %H:%M:%S'): $message${no_color}"
    fi
}

# Define a function to get message type based on log level
function get_msg_type_for_level() {
    local level=$1
    case $level in
        1) echo "DEBUG" ;;
        2) echo "INFO" ;;
        3) echo "WARNING" ;;
        4) echo "ERROR" ;;
        5) echo "CRITICAL" ;;
        *) echo "INFO" ;;  # Default to INFO if level is out of range
    esac
}

function update_next_level_msg_type() {
    next_level_msg_type=$(get_msg_type_for_level $((LOG_LEVEL + 1)))
}

function setup_log_level() {
    local level=$1
    LOG_LEVEL=${level:-0}  # Default to 0 if no argument is provided
    update_next_level_msg_type
}