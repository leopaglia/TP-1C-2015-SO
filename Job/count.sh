for f in "${@:-/dev/stdin}"; do
	words=( $(< "$f") )
	(( num += ${#words[@]} ))
done
echo Word count: $num.