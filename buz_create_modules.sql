--
-- Скрипт сгенерирован Devart dbForge Studio for Oracle, Версия 3.6.389.0
-- Домашняя страница продукта: http://www.devart.com/ru/dbforge/oracle/studio
-- Дата скрипта: 10.03.2018 22:34:36
-- Версия сервера: Oracle Database 11g Express Edition Release 11.2.0.2.0 - Production
-- Версия клиента: 
--


CREATE OR REPLACE TYPE BUZ.SYS_PLSQL_20061_DUMMY_1 AS
TABLE OF NUMBER;
/

CREATE OR REPLACE TYPE BUZ.SYS_PLSQL_20061_39_1 AS
TABLE OF BUZ.SYS_PLSQL_20061_9_1;
/

CREATE OR REPLACE PACKAGE BUZ.monthly_income_pkg
  AS
  --20180127 created
  --20180203 добавлена пайплайн ф-я для возврата записей аналогично хп
  TYPE income_rt IS RECORD (
      period monthly_income.period % TYPE,
      item   income_item.item % TYPE,
      total  monthly_income.total % TYPE
    );
  TYPE income_tt IS TABLE OF income_rt;
  --  PROCEDURE get_income_monthly_rs(p_recordset OUT SYS_REFCURSOR);
  PROCEDURE get_income_monthly_rs(d_start_in      DATE,
                                  d_end_in        DATE,
                                  p_recordset OUT SYS_REFCURSOR);

  FUNCTION get_income_monthly_pipe(d_start_in DATE,
                                   d_end_in   DATE)
    RETURN income_tt
  PIPELINED;
END MONTHLY_INCOME_PKG;

/

CREATE OR REPLACE PACKAGE BODY BUZ.monthly_income_pkg
  AS

  FUNCTION get_income_monthly_pipe(d_start_in DATE,
                                   d_end_in   DATE)
    RETURN income_tt
  PIPELINED
    IS
    BEGIN
      FOR this_cursor IN (SELECT period,
                                 item,
                                 total
          FROM v_monthly_income_group
          WHERE 1 = 1
          --AND period BETWEEN d_start_in AND d_end_in
          ORDER BY period DESC)
      LOOP
        PIPE ROW (this_cursor);
      END LOOP;
    --DBMS_OUTPUT.PUT_LINE(var_returned_rows_count|| ' rows affected');
    --тест функции---------------------------------------------------------
    --select * from table(MONTHLY_INCOME_PKG.get_income_monthly_pipe
    --      (TO_DATE('20180101','yyyymmdd'), TO_DATE('20180201','yyyymmdd')))
    --/тест----------------------------------------------------------------
    END get_income_monthly_pipe;
  --
  --PROCEDURE get_income_monthly_rs(p_recordset OUT SYS_REFCURSOR)
  --  AS
  --  BEGIN
  --    OPEN p_recordset FOR
  --    SELECT 
  --      --      period, item, total
  --      *
  --    FROM v_monthly_income_group;
  ----    WHERE period >=
  ----    ORDER BY period desc;
  --  END get_income_monthly_rs;

  PROCEDURE get_income_monthly_rs(d_start_in      DATE,
                                  d_end_in        DATE,
                                  p_recordset OUT SYS_REFCURSOR)
    AS
    BEGIN
      OPEN p_recordset FOR
      SELECT period,
             item,
             total
        FROM v_monthly_income_group
        WHERE 1 = 1
          AND period BETWEEN d_start_in AND d_end_in
        ORDER BY period DESC;
    END get_income_monthly_rs;

END MONTHLY_INCOME_PKG;

/

CREATE OR REPLACE PACKAGE BUZ.calc_sld_pkg
  AS
  PROCEDURE calc_sld(period_in IN SLD.PERIOD % TYPE);
END calc_sld_pkg;

/

CREATE OR REPLACE PACKAGE BODY BUZ.calc_sld_pkg
  AS

  PROCEDURE calc_sld(period_in IN SLD.PERIOD % TYPE)
    --20180129
    --20180219 добавлен пересчет приходов за оп в случае, если нет записей в sld за этот оп
    AS
      l_balance_row_count NUMBER;
      l_balance_max_date  SLD.PERIOD % TYPE;
    BEGIN
      --2DO разбить апдейт и инсерт на 2 процедуры

      SELECT COUNT(*)
        INTO l_balance_row_count
        FROM sld s
        WHERE s.PERIOD = period_in;

      --если уже есть сальдообороты за этот ОП - апдейт
      IF l_balance_row_count > 0
      THEN
        --  пересчет приходов за оп 
        fill_monthly_income_pkg.fill_monthly_income(period_in, period_in);

        --обновить сальдо нач и кон
        UPDATE SLD s
          SET BSLD = (SELECT b_value FROM balance b WHERE 1 = 1 AND b.B_DATE = s.PERIOD AND b.CURRENCY_ID = s.CURRENCY_ID), ESLD = (SELECT b_value FROM balance b WHERE 1 = 1 AND b.B_DATE = ADD_MONTHS(s.PERIOD, 1) AND b.CURRENCY_ID = s.CURRENCY_ID)
          WHERE s.PERIOD = period_in;

        --обновить приходы 
        UPDATE SLD s
          SET s.TOTAL_INCOME =
          --    (select SUM (INCOME_VALUE)  FROM income_fact f 
          --        WHERE s.PERIOD = TRUNC(F_DATE, 'MM') AND f.CURRENCY_ID = s.CURRENCY_ID)
          (SELECT total FROM monthly_income i WHERE i.CURRENCY_ID = s.CURRENCY_ID AND i.period = period_in)
          WHERE s.PERIOD = period_in;

        --обновить расходы
        UPDATE SLD s
          SET s.COSTS = BSLD - ESLD + TOTAL_INCOME
          WHERE s.PERIOD = period_in;
      ELSE   --расчет за новый оп
        SELECT MAX(period)
          INTO l_balance_max_date
          FROM SLD;
        IF l_balance_max_date = ADD_MONTHS(period_in, -1)
        THEN
          --пересчет приходов за оп 
          fill_monthly_income_pkg.fill_monthly_income(period_in, period_in);

          --данные в новый оп
          INSERT INTO SLD (
            PERIOD, CURRENCY_ID, BSLD, TOTAL_INCOME, COSTS, ESLD
          )
            SELECT period_in,
                   s.CURRENCY_ID,
                   s.ESLD AS bsld,
                   i.total,
                   s.ESLD + NVL(i.total, 0) - b.B_VALUE,
                   b.B_VALUE
              FROM SLD s
                --JOIN v_monthly_income_total i 
                JOIN monthly_income i --20180130
                  ON i.period = ADD_MONTHS(s.period, 1)
                  AND s.currency_id = i.currency_id
                  JOIN BALANCE b
                    ON s.currency_id = b.CURRENCY_ID
                    AND b.b_date = ADD_MONTHS(s.period, 2)
              WHERE 1 = 1
                AND i.period = period_in;
        ELSE --выбран инвалидный оп 2DO переделать на рейз экспешн
          DBMS_OUTPUT.PUT_LINE(TO_CHAR(period_in, 'yyyymmdd') || ' является инвалидным периодом');
        END IF;
      END IF;
    END calc_sld;

END CALC_SLD_PKG;

/

CREATE OR REPLACE PACKAGE BUZ.fill_ASSET_BALANCE_pkg
  AS
  PROCEDURE fill_ASSET_BALANCE(d_start_in IN SLD.PERIOD % TYPE,
                               d_end_in   IN SLD.PERIOD % TYPE);
END fill_ASSET_BALANCE_pkg;

/

CREATE OR REPLACE PACKAGE BODY BUZ.fill_ASSET_BALANCE_pkg
  AS

  PROCEDURE fill_ASSET_BALANCE(d_start_in IN SLD.PERIOD % TYPE,
                               d_end_in   IN SLD.PERIOD % TYPE)
    --20180131 обновление таблицы ASSET_BALANCE
    AS
    BEGIN
      MERGE INTO ASSET_BALANCE i
      USING
      (SELECT EQ_DATE,
              CURRENCY_ID,
              SUM(EQ_VALUE) AS b_value

          FROM RTR_EQUITY e
          WHERE EQ_DATE BETWEEN d_start_in AND d_end_in
          GROUP BY e.EQ_DATE,
                   CURRENCY_ID) f
      ON (i.b_date = f.eq_date AND i.currency_id = f.CURRENCY_ID)
      WHEN MATCHED THEN UPDATE SET i.b_value = f.b_value
      WHEN NOT MATCHED THEN INSERT (i.b_date, i.currency_id, i.b_value)
      VALUES (f.EQ_DATE, f.currency_id, f.b_value);
    END fill_ASSET_BALANCE;

END fill_ASSET_BALANCE_pkg;

/

CREATE OR REPLACE PACKAGE BUZ.fill_monthly_income_pkg
  AS
  PROCEDURE fill_monthly_income(d_start_in IN SLD.PERIOD % TYPE,
                                d_end_in   IN SLD.PERIOD % TYPE);
END fill_monthly_income_pkg;

/

CREATE OR REPLACE PACKAGE BODY BUZ.fill_monthly_income_pkg
  AS

  PROCEDURE fill_monthly_income(d_start_in IN SLD.PERIOD % TYPE,
                                d_end_in   IN SLD.PERIOD % TYPE)
    --20180130 обновление таблицы сводных данных по ежемесячным доходам
    --процедуру запускать еженедельно и по требованию
    AS
    BEGIN
      MERGE INTO monthly_income i
      USING
      (SELECT TRUNC(F_DATE, 'MM') period,
              SUM(INCOME_VALUE) total,
              c.CURRENCY_ID,
              c.CURRENCY_TICKER,
              ADD_MONTHS(TRUNC(F_DATE, 'MM'), 1) b_date
          FROM income_fact f
            JOIN CURRENCY c
              ON c.CURRENCY_ID = f.CURRENCY_ID
          WHERE TRUNC(F_DATE, 'MM') BETWEEN d_start_in AND d_end_in
          GROUP BY c.CURRENCY_TICKER,
                   c.CURRENCY_ID,
                   TRUNC(F_DATE, 'MM')) f
      ON (i.period = f.period AND i.currency_id = f.CURRENCY_ID)
      WHEN MATCHED THEN UPDATE SET i.total = f.total
      WHEN NOT MATCHED THEN INSERT (i.period, i.total, i.currency_id, i.currency_ticker, i.b_date)
      VALUES (f.period, f.total, f.currency_id, f.currency_ticker, f.b_date);
    END fill_monthly_income;

END fill_monthly_income_pkg;

/

CREATE OR REPLACE PROCEDURE BUZ.recalc_all(period_in IN SLD.PERIOD % TYPE)
  --20180220 пересчёт всех нужных таблиц - запуск как по запросу
  -- и после окончания ОП 
  AS
  BEGIN
    fill_ASSET_BALANCE_pkg.fill_ASSET_BALANCE(period_in, period_in);
    fill_monthly_income_pkg.fill_monthly_income(period_in, period_in);
    CALC_SLD_PKG.CALC_SLD(period_in);
  END;
/

CREATE OR REPLACE PROCEDURE BUZ.contact_us
-- Create a wrapper procedure for apex_mail
--
(p_from IN VARCHAR2,
 p_body IN VARCHAR2)
  AS
  BEGIN
    apex_mail.send(
    p_from => p_from,
    p_to => 'temp_mail@e1.ru',
    p_subj => 'Message from the APEX Issue Tracker',
    p_body => p_body,
    p_body_html => p_body);
  END contact_us;
/

CREATE OR REPLACE PACKAGE get_rtr_data_pkg AS
TYPE twr_rt IS RECORD 
    (t_date date 
    , fy real 
    , twr1 REAL
    , twr2 REAL
    , twr3 REAL
    , twr_f real
    );
TYPE twr_tt is table of twr_rt;

    function get_fact_twr_pipe (d_start_in DATE, d_end_in DATE)
    return twr_tt
    pipelined;

END get_rtr_data_pkg;

CREATE OR REPLACE PACKAGE BODY get_rtr_data_pkg AS

 function get_fact_twr_pipe (d_start_in DATE, d_end_in DATE)
    return twr_tt
    pipelined
IS
begin
for this_cursor in
  (
    SELECT 
    y.YIELD_DATE AS d, y.fact_yield
    ,EXP(SUM(LN(1+theor_YIELD_N1))over (order by yield_date) ) twr_n1
    ,EXP(SUM(LN(1+theor_YIELD_N2))over (order by yield_date) ) twr_n2
    ,EXP(SUM(LN(1+theor_YIELD_N3))over (order by yield_date) ) twr_n3 
  , EXP(SUM(LN(1+FACT_YIELD)) over (order by yield_date) )twr_fact
    --select *
    from RTR_YIELD y
    where 1 = 1
    and y.YIELD_DATE BETWEEN d_start_in AND d_end_in
  )
loop
pipe row (this_cursor);
--тест функции---------------------------------------------------------
select * from table(get_rtr_data_pkg.get_fact_twr_pipe
      (TO_DATE('20180101','yyyymmdd'), TO_DATE('20180401','yyyymmdd')));
--/тест----------------------------------------------------------------
end loop;

END GET_FACT_TWR_PIPE;

END get_rtr_data_pkg;