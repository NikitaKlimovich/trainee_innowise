while [[ "$#" -gt 0 ]]; do
    case $1 in
        -r|--rooms) rooms="$2"; shift ;;
        -s|--students) students="$2"; shift ;;
        -f|--format) format="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done
pip install -r requirements.txt
python main.py -r $rooms -s $students -f $format