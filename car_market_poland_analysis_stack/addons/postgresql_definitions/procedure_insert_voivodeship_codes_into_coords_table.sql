CREATE OR REPLACE PROCEDURE public.updateVoivodeshioCoordsTableKeys()
LANGUAGE SQL
AS $$
UPDATE dict_voivodeships_coords AS v1
SET cepik_voivodeship_code = v2.klucz_slownika
FROM dict_voivodeships AS v2
WHERE LOWER(v1.name) = LOWER(v2.wartosc_slownika)
$$;