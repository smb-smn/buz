--select * from table(BUz.get_rtr_data_pkg.get_fact_twr_pipe
--      (TO_DATE('20180301','yyyymmdd'), TO_DATE('20180401','yyyymmdd')));
CREATE or REPLACE FUNCTION max_dd_standalone(
  d_start_in DATE, d_end_in DATE
  --,twr_x VARCHAR(20)
  , twr_type_in int) RETURN NUMBER
  AS CURSOR twr_cur is
  select t_date
  ,case twr_type_in
  when 0 then twr_f when 1 then twr1 when 2 then twr2 when 3 then twr3 end twr
  from  table(BUZ.get_rtr_data_pkg.get_fact_twr_pipe(d_start_in, d_end_in) ) ;
  twr_rec twr_cur%ROWTYPE;
  max_eq NUMBER; max_dd NUMBER; dd NUMBER; dd_date DATE;
  retval NUMBER;

  BEGIN
  max_eq:=1; max_dd:=0;
  OPEN twr_cur;
  LOOP
  FETCH twr_cur INTO twr_rec;
  EXIT WHEN twr_cur%NOTFOUND;

--  IF twr_cur%FOUND
--    THEN
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
  RETURN retval;
----тест функции---------------------------------------------------------
--select BUZ.max_dd_standalone
--      (TO_DATE('20180301','yyyymmdd'), TO_DATE('20180401','yyyymmdd'),0) from dual;
----/тест--
  END;