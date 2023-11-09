SELECT COUNT(DISTINCT age)
FROM heart_disease;

SELECT AVG(age) AS avg_age
FROM heart_disease
GROUP BY sex;

SELECT sex, num, AVG(age) AS avg_age
FROM heart_disease
GROUP BY sex, num;