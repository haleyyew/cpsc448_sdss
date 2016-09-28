awk -F, '$1 ~ /\"100000\"/ { print }' session.csv
awk -F, '$1 == 100000 { print }' sessionlog.csv
awk -F, '$1 ~ /\"12825\"/ { print }' ipAll.csv
awk -F, '$10 == 12825 { print }' sqllog.csv
awk -F, '$1 == 48063987 || $1 == 48390699 { print;for(i=0; i<=100; i++) { getline;print;} }' sqlstatement.csv
