-- Test foreign key relationship between sec_2016q4_cf and sec_2016q4_main
SELECT MAIN_ID FROM {{ref ('sec_2016q4_cf')}}
WHERE MAIN_ID NOT IN (SELECT MAIN_ID FROM {{ref ('sec_2016q4_main')}})