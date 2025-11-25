DO $$
DECLARE
    start_date DATE := '2025-01-01';
    end_date DATE := '2027-12-31';
    current_date DATE := start_date;
BEGIN
    TRUNCATE public.dimdate RESTART IDENTITY;

    WHILE current_date <= end_date LOOP
        INSERT INTO public.dimdate (
            date_key,
            full_date,
            day_of_week_name,
            day_of_month,
            month_name,
            month_of_year,
            calendar_quarter,
            calendar_year,
            is_weekend
        )
        VALUES (
            CAST(TO_CHAR(current_date, 'YYYYMMDD') AS INT),
            current_date,
            CASE EXTRACT(DOW FROM current_date)
                WHEN 0 THEN 'Domingo'
                WHEN 1 THEN 'Segunda'
                WHEN 2 THEN 'Terça'
                WHEN 3 THEN 'Quarta'
                WHEN 4 THEN 'Quinta'
                WHEN 5 THEN 'Sexta'
                WHEN 6 THEN 'Sábado'
            END,
            EXTRACT(DAY FROM current_date),
            CASE EXTRACT(MONTH FROM current_date)
                WHEN 1 THEN 'Janeiro'
                WHEN 2 THEN 'Fevereiro'
                WHEN 3 THEN 'Março'
                WHEN 4 THEN 'Abril'
                WHEN 5 THEN 'Maio'
                WHEN 6 THEN 'Junho'
                WHEN 7 THEN 'Julho'
                WHEN 8 THEN 'Agosto'
                WHEN 9 THEN 'Setembro'
                WHEN 10 THEN 'Outubro'
                WHEN 11 THEN 'Novembro'
                WHEN 12 THEN 'Dezembro'
            END,
            EXTRACT(MONTH FROM current_date),
            EXTRACT(QUARTER FROM current_date),
            EXTRACT(YEAR FROM current_date),
            CASE WHEN EXTRACT(DOW FROM current_date) IN (0, 6) THEN TRUE ELSE FALSE END
        );
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;
