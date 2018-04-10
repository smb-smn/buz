--
-- Скрипт сгенерирован Devart dbForge Studio for Oracle, Версия 3.6.389.0
-- Домашняя страница продукта: http://www.devart.com/ru/dbforge/oracle/studio
-- Дата скрипта: 10.04.2018 19:21:34
-- Версия сервера: Oracle Database 11g Express Edition Release 11.2.0.2.0 - Production
-- Версия клиента: 
--


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

CREATE OR REPLACE PACKAGE BUZ.get_rtr_data_pkg
  AS
  TYPE twr_rt IS RECORD (
      t_date DATE,
      fy     REAL,
      twr1   REAL,
      twr2   REAL,
      twr3   REAL,
      twr_f  REAL
    );
  TYPE twr_tt IS TABLE OF twr_rt;
  TYPE drawdown_rt IS RECORD (
      dd_date  DATE,
      dd_value REAL
    );

  FUNCTION max_drawdown(d_start_in  DATE,
                        d_end_in    DATE,
                        twr_type_in INT)
    RETURN drawdown_rt;

  FUNCTION get_fact_twr_pipe(d_start_in DATE,
                             d_end_in   DATE)
    RETURN twr_tt
  PIPELINED;

  FUNCTION twr(d_start_in DATE,
               d_end_in   DATE)
    RETURN REAL;

  FUNCTION profit(d_start_in     DATE,
                  d_end_in       DATE,
                  balance_in     REAL,
                  proposal_id_in RTR_PROPOSAL.PROPOSAL_ID % TYPE)
    RETURN REAL;

END get_rtr_data_pkg;

/

CREATE OR REPLACE PACKAGE BODY BUZ.get_rtr_data_pkg
  AS

  FUNCTION get_fact_twr_pipe(d_start_in DATE,
                             d_end_in   DATE)
    RETURN twr_tt
  PIPELINED
    --20180410 добавлена ф-я получения макс дродауна за выбр.период
    --20180331 учтена возможность существования нулл-доходности
    IS
    BEGIN
      FOR this_cursor IN (SELECT y.YIELD_DATE AS d,
                                 y.fact_yield,
                                 EXP(SUM(LN(1 + COALESCE(theor_YIELD_N1, 0))) OVER (ORDER BY yield_date)) twr_n1,
                                 EXP(SUM(LN(1 + COALESCE(theor_YIELD_N2, 0))) OVER (ORDER BY yield_date)) twr_n2,
                                 EXP(SUM(LN(1 + COALESCE(theor_YIELD_N3, 0))) OVER (ORDER BY yield_date)) twr_n3,
                                 EXP(SUM(LN(1 + COALESCE(FACT_YIELD, 0))) OVER (ORDER BY yield_date)) twr_fact
          --select *
          FROM RTR_YIELD y
          WHERE 1 = 1
            AND y.YIELD_DATE BETWEEN d_start_in AND d_end_in)
      LOOP
        PIPE ROW (this_cursor);
      --тест функции---------------------------------------------------------
      --select * from table(BUZ.get_rtr_data_pkg.get_fact_twr_pipe
      --      (TO_DATE('20180301','yyyymmdd'), TO_DATE('20180401','yyyymmdd')));
      --/тест----------------------------------------------------------------
      END LOOP;

    END GET_FACT_TWR_PIPE;


  FUNCTION max_drawdown(d_start_in  DATE,
                        d_end_in    DATE,
                        twr_type_in INT)
    RETURN drawdown_rt
    --20180410 возврат даты и значения макс. дродауна за выбранный период для выбранного 1-го из 4-х временного ряда
    IS
      CURSOR twr_cur IS
          SELECT t_date,
                 CASE twr_type_in WHEN 0 THEN twr_f WHEN 1 THEN twr1 WHEN 2 THEN twr2 WHEN 3 THEN twr3 END twr
            FROM TABLE (BUZ.get_rtr_data_pkg.get_fact_twr_pipe(d_start_in, d_end_in));
      twr_rec      twr_cur % ROWTYPE;
      drawdown_rec drawdown_rt;
      max_eq       NUMBER;
      max_dd       NUMBER;
      dd           NUMBER;
      dd_date      DATE;
      retval       NUMBER;

    BEGIN
      max_eq := 1;
      max_dd := 0;
      OPEN twr_cur;
      LOOP
        FETCH twr_cur INTO twr_rec;
        EXIT WHEN twr_cur % NOTFOUND;

        IF max_eq < twr_rec.twr
        THEN
          max_eq := twr_rec.twr;
        ELSE
          DD := (twr_rec.twr - max_Eq) / max_Eq;
        END IF;
        IF max_dd > dd
        THEN
          max_DD := dd;
          dd_date := twr_rec.t_date;
        END IF;

      END LOOP;
      CLOSE twr_cur;
      drawdown_rec.dd_value := -1 * max_dd;
      drawdown_rec.dd_date := dd_date;
      --DBMS_OUTPUT.PUT_LINE(DD_date || '  dd= ' ||drawdown_rec.dd_value);
      RETURN drawdown_rec;
      --тест функции---------------------------------------------------------
      --select BUZ.get_rtr_data_pkg.max_drawdown
      --      (TO_DATE('20180301','yyyymmdd'), TO_DATE('20180401','yyyymmdd'),0).dd_date from dual;
      BEGIN
        DBMS_OUTPUT.PUT_LINE(BUZ.get_rtr_data_pkg.max_drawdown
        (TO_DATE('20180301', 'yyyymmdd'), TO_DATE('20180320', 'yyyymmdd'), 0).dd_date);
        DBMS_OUTPUT.PUT_LINE(BUZ.get_rtr_data_pkg.max_drawdown
        (TO_DATE('20180301', 'yyyymmdd'), TO_DATE('20180320', 'yyyymmdd'), 0).dd_value);
      END;
    --/тест--
    END;

  FUNCTION twr(d_start_in DATE,
               d_end_in   DATE)

    RETURN REAL
    --20180331 учтена возможность существования нулл-доходности
    IS
      retval REAL;
    BEGIN
      SELECT EXP(SUM(LN(1 + COALESCE(FACT_YIELD, 0))))
        INTO retval
        FROM RTR_YIELD
        WHERE 1 = 1
          AND YIELD_DATE BETWEEN d_start_in AND d_end_in;
      RETURN retval;
    ----тест ф-ии
    --begin
    --DBMS_OUTPUT.PUT_LINE(get_rtr_data_pkg.twr(TO_DATE('20180101','yyyymmdd'), TO_DATE('20180401','yyyymmdd')));
    --END;

    END twr;

  FUNCTION profit(d_start_in     DATE,
                  d_end_in       DATE,
                  balance_in     REAL,
                  proposal_id_in RTR_PROPOSAL.PROPOSAL_ID % TYPE)
    RETURN REAL
    --
    IS
      retval    REAL;
      balance0  REAL;
      balance_  REAL;
      fee_      REAL;
      equity    REAL;
      twr_      REAL;
      i         INT;
      d1        DATE;
      d2        DATE;
      interval_ RTR_PROPOSAL.INTERVAL_MONTH % TYPE;

    BEGIN
      --2DO убрать лишнюю переменную balance0
      i := 1;
      balance0 := balance_in;
      d1 := d_start_in;
      d2 := d_start_in;
      equity := balance_in;
      balance_ := balance_in;
      SELECT MAX(INTERVAL_MONTH)
        INTO interval_
        FROM RTR_PROPOSAL
        WHERE proposal_id = proposal_id_in;
      SELECT p.FEE / 100
        INTO fee_
        FROM RTR_PROPOSAL p
        WHERE 1 = 1
          AND proposal_id = proposal_id_in
          AND p.BALANCE = (SELECT MIN(BALANCE)
              FROM RTR_PROPOSAL p2
              WHERE p2.BALANCE >= equity);

      --  DBMS_OUTPUT.PUT_LINE('fee='||fee_||' interval='|| interval_);

      WHILE (ADD_MONTHS(d_start_in, i * interval_) < d_end_in)
        LOOP
          d2 := ADD_MONTHS(d_start_in, i * interval_) - 1 * 24;
          twr_ := twr(d1, d2);
          equity := equity * twr_; --средства без учета фии

          DBMS_OUTPUT.PUT_LINE('twr_=' || twr_ || ' equity=' || equity);

          --IF twr_ > 1 AND THEN 
          IF equity - balance_ > 0
          THEN --есть прирост средств за расчетный период
            equity := equity - (equity - balance_) * fee_; --учли фии
            balance_ := equity;
            SELECT p.FEE / 100
              INTO fee_ --поиск значения фии для следующего периода
              FROM RTR_PROPOSAL p
              WHERE 1 = 1
                AND p.BALANCE = (SELECT MIN(BALANCE)
                    FROM RTR_PROPOSAL p2
                    WHERE p2.BALANCE >= balance_);
          END IF;
          i := i + 1;
        END LOOP;
      d1 := d2;
      d2 := d_end_in - 1 * 24;
      twr_ := twr(d1, d2);
      equity := equity * twr_;
      IF equity - balance_ > 0
      THEN --есть прирост средств за расчетный период
        equity := equity - (equity - balance_) * fee_;
      END IF;

      DBMS_OUTPUT.PUT_LINE('twr_=' || twr_ || ' equity=' || equity);

      retval := equity - balance_in;
      RETURN retval;
    END PROFIT;

END get_rtr_data_pkg;

/

CREATE OR REPLACE FUNCTION BUZ.max_dd_standalone(
  d_start_in DATE, d_end_in DATE
  --,twr_x VARCHAR(20)
  , twr_type_in int) 
 RETURN NUMBER
  --RETURN twr_rec
  AS CURSOR twr_cur is
  select t_date
  ,case twr_type_in
  when 0 then twr_f when 1 then twr1 when 2 then twr2 when 3 then twr3 end twr
  from  table(BUZ.get_rtr_data_pkg.get_fact_twr_pipe(d_start_in, d_end_in) ) ;
  twr_rec twr_cur%ROWTYPE;
  max_eq NUMBER; max_dd NUMBER; dd NUMBER; dd_date DATE;
  retval NUMBER;
  --retval twr_cur%ROWTYPE;

  BEGIN
  max_eq:=1; max_dd:=0;
  OPEN twr_cur;
  LOOP
  FETCH twr_cur INTO twr_rec;
  EXIT WHEN twr_cur%NOTFOUND;

      IF max_eq < twr_rec.twr THEN 
        max_eq := twr_rec.twr;
      else 
        DD := (twr_rec.twr - max_Eq) / max_Eq;
      END if;
      IF max_dd > dd THEN
        max_DD := dd;
		    dd_date := twr_rec.t_date;
      END IF;
--    FETCH twr_cur INTO twr_rec;
--  END IF; 
  END LOOP;
  CLOSE twr_cur;
  retval := -1* max_dd;
  DBMS_OUTPUT.PUT_LINE(DD_date || '  dd= ' ||retval);
  --retval.twr :=-1* max_dd;retval.t_date := twr_rec.t_date;
  RETURN retval;
----тест функции---------------------------------------------------------
--select BUZ.max_dd_standalone
--      (TO_DATE('20180301','yyyymmdd'), TO_DATE('20180401','yyyymmdd'),0) from dual;
----/тест--
  END;
/