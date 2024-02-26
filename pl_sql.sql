CREATE OR REPLACE PACKAGE ivgaide IS

TYPE t_msp_record IS RECORD
    (
     ICUSNUM gis_msp_ch.icusnum%TYPE,
     CCUSNUMNAL cus.ccusnumnal%TYPE,
     MSPDATE gis_msp_ch.MSPDATE%TYPE,
     MSPTYPE gis_msp_ch.MSPTYPE%TYPE
    );

  TYPE t_msp_registry IS TABLE OF t_msp_record;

FUNCTION msp_update_3
    RETURN t_log_tab;
END;

CREATE OR REPLACE PACKAGE BODY ivgaide IS

FUNCTION msp_update_3 RETURN t_log_tab IS
         PRAGMA AUTONOMOUS_TRANSACTION;

/*По каждому клиенту сохраняем все статусы МСП в коллекцию client_statuses. Далее по данным этой коллекции проверяем текущие значения
доп. атрибута 352 клиента.
Если есть различия между коллекцией и доп. атрибутом, то корректируем доп. атрибут.
Если есть статус в полученной коллекции, но нет такого доп. атрибута, то добавляем доп. атрибут.
Если в коллекции значения нет, но кто-то добавил такой доп. атрибут, то такой доп. атрибут удаляем.*/

         my_table            t_log_tab;
         client_statuses     t_msp_registry;
         msp_registry        t_msp_registry;
         v_cur_record        t_msp_record;
         
         cur_icusnum cus.icusnum%TYPE := 0;
                  
         PROCEDURE p_add_attributes (p_nested_table IN t_msp_registry) IS
           PRAGMA AUTONOMOUS_TRANSACTION;
             
           msp_all_values        t_msp_registry;
           v_id_value            CUS_ADD_ATR.ID_VALUE%TYPE;
           v_cur_value           varchar2(100);
           v_iteration           NUMBER;
           v_exists              NUMBER(1);
          
           TYPE t_atr_record IS RECORD
            (
             DATE_VALUE  cus_add_atr_val.date_value%TYPE,     
             CVALUE      cus_add_atr_val.CVALUE%TYPE,
             INSERTED_BY cus_add_atr_val.INSERTED_BY%TYPE
            );

           TYPE t_all_atr_values IS TABLE OF t_atr_record;
           t_all_atrs t_all_atr_values;
           
           CURSOR check_extra_values(p_id_value CUS_ADD_ATR.ID_VALUE%TYPE) IS 
                  SELECT cvalue, date_value FROM cus_add_atr_val where id_value=p_id_value
                  FOR UPDATE;
                  
            BEGIN
              v_id_value := cus_attr_utl.get_attr_id_by_cus(p_nested_table(1).icusnum, 352);  
              
              IF p_nested_table(1).msptype <> 'Не включался в МСП' THEN /*если у клиента есть статусы МСП в реестре*/       
                FOR i IN p_nested_table.FIRST..p_nested_table.LAST LOOP
                 IF v_id_value IS NOT NULL THEN /*уже есть значения доп. атрибута МСП в АБС*/
                   BEGIN
                     SELECT CVALUE INTO v_cur_value FROM cus_add_atr_val WHERE id_value=v_id_value AND DATE_VALUE = p_nested_table(i).mspdate;
                     IF v_cur_value <> p_nested_table(i).msptype THEN /*если доп. атрибут в АБС отличается от статуса МСП, то корректируем доп. атрибут*/
                       UPDATE cus_add_atr_val SET CVALUE = p_nested_table(i).msptype WHERE id_value=v_id_value AND DATE_VALUE = p_nested_table(i).mspdate;
                       p_log_msg(my_table, 'SUCCESS', p_nested_table(i).icusnum, p_nested_table(i).ccusnumnal,  utl_lms.format_message(q'[Значение "%s" заменено на "%s" от %s%]', v_cur_value, p_nested_table(i).msptype, to_char(p_nested_table(i).mspdate, 'dd.mm.yyyy')), 'МСП', USER);
                     END IF;
                         
                     EXCEPTION
                        WHEN NO_DATA_FOUND THEN /*если такого доп. атрибута нет, то добавляем его*/
                          INSERT INTO cus_add_atr_val (ID_VALUE, DATE_VALUE, CVALUE, INSERTED_BY ) values (v_id_value, p_nested_table(i).mspdate, p_nested_table(i).msptype, USER);
                          p_log_msg(my_table, 'SUCCESS', p_nested_table(i).icusnum, p_nested_table(i).ccusnumnal, utl_lms.format_message(q'[Добавлен статус "%s" датой %s]', p_nested_table(i).msptype, to_char(p_nested_table(i).mspdate, 'dd.mm.yyyy')), 'МСП', USER);
                             
                        WHEN OTHERS THEN
                           p_log_msg(my_table, 'ERROR', p_nested_table(i).icusnum, p_nested_table(i).ccusnumnal, SQLERRM, 'МСП', USER);
                           ROLLBACK;
                   END;
                 ELSE /*ставим доп. атрибут МСП первый раз*/
                   cus_attr_hdr.set_attr_val_s(p_nested_table(i).icusnum, 352, p_nested_table(i).msptype, p_nested_table(i).mspdate);
                 END IF;
                END LOOP;
                
                /*удаляем лишние значения, если есть. Через курсор берем все значения доп. атрибутов*/              
                FOR cur_status IN check_extra_values(v_id_value) LOOP
                  v_exists    := 0;
                  v_iteration := p_nested_table.FIRST;
                  
                   LOOP 
                      IF v_exists = 1 OR v_iteration IS NULL THEN
                        EXIT;
                      END IF;
                      IF p_nested_table(v_iteration).mspdate = cur_status.date_value AND p_nested_table(v_iteration).msptype = cur_status.cvalue THEN
                        v_exists  := 1;
                      END IF;
                      v_iteration := p_nested_table.NEXT(v_iteration);

                    END LOOP;
                  
                  /*полученное значение из курсора check_extra_values есть в доп. атрибутах, но нет в коллекции статусов МСП клиента. Значит удаляем его из доп. атрибутов*/ 
                  IF v_exists = 0 THEN
                      DELETE FROM cus_add_atr_val WHERE CURRENT OF check_extra_values;
                      p_log_msg(my_table, 'SUCCESS', p_nested_table(1).icusnum, p_nested_table(1).ccusnumnal, utl_lms.format_message(q'[Удалено лишнее значение "%s" от %s]', cur_status.cvalue, to_char(cur_status.date_value, 'dd.mm.yyyy')), 'МСП', USER);            
                  END IF;
                END LOOP;
                
              ELSE /*если он никогда не был в реестре МСП*/
               /*если v_id_value не пустой, то кто-то когда-то добавлял в доп. атрибут какое-то значение*/
               IF v_id_value IS NOT NULL THEN
                 t_all_atrs := t_all_atr_values(); 
                 DELETE FROM cus_add_atr_val WHERE id_value = v_id_value RETURNING DATE_VALUE, CVALUE, INSERTED_BY BULK COLLECT INTO t_all_atrs;
                 FOR i IN t_all_atrs.first .. t_all_atrs.last LOOP
                    p_log_msg(my_table, 'SUCCESS', p_nested_table(1).icusnum, p_nested_table(1).ccusnumnal, utl_lms.format_message(q'[Удалено лишнее значение "%s" от %s. Ставил %s]', t_all_atrs(i).CVALUE, to_char(t_all_atrs(i).DATE_VALUE, 'dd.mm.yyyy'), t_all_atrs(i).INSERTED_BY) , 'МСП', USER);
                 END LOOP;
               END IF;  
                 
              END IF;
              
             COMMIT;

             EXCEPTION
                WHEN OTHERS THEN
                   p_log_msg(my_table, 'ERROR', null, null, SQLERRM, 'МСП', USER);
                   ROLLBACK;
              
            END;
           
           PROCEDURE p_delete_statuses (p_nested_table IN OUT t_msp_registry) IS
            BEGIN
              p_nested_table.DELETE;
            END;
           BEGIN

          my_table := t_log_tab();
          client_statuses := t_msp_registry();
          
          SELECT g.ICUSNUM, c.ccusnumnal, g.MSPDATE, g.MSPTYPE BULK COLLECT INTO msp_registry from gis_msp_ch g left join cus c on g.icusnum = c.icusnum order by g.ICUSNUM /*offset 10 rows fetch next 20 rows only*/;
          
          /*проходим всю таблицу со значениями статусов МСП*/
          FOR i IN 1..msp_registry.count LOOP
            /*в ней клиенты идут по порядку. Если:
                                    - по текущей строке icusnum отличается от icusnum прошлой строки,
                                       или
                                    - последний клиент в таблице статусов МСП
            то мы очищаем коллекцию статусов МСП прошлого клиента и сохраняем в нее статусы МСП текущего клиента*/
            IF msp_registry(i).icusnum <> cur_icusnum OR i = msp_registry.COUNT THEN
              cur_icusnum := msp_registry(i).icusnum;
              
              IF client_statuses.COUNT > 0 THEN
                p_add_attributes(client_statuses);
                p_delete_statuses(client_statuses);
              END IF;
            END IF;
            
            /*сохраняем статусы МСП клиента в коллекцию*/
            v_cur_record.ICUSNUM := cur_icusnum;
            v_cur_record.CCUSNUMNAL := msp_registry(i).ccusnumnal;
            v_cur_record.MSPDATE := msp_registry(i).mspdate;
            v_cur_record.MSPTYPE := msp_registry(i).msptype;
            client_statuses.EXTEND;
            client_statuses(client_statuses.LAST) := v_cur_record;              
          END LOOP;

          return my_table;
  END;

END;
