CREATE MATERIALIZED VIEW mv_suma_kraj AS
SELECT kraj, sum(kwota) AS suma
FROM zrodlo_zamowienia
GROUP BY kraj;
