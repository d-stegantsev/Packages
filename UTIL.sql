--Специфікація
create or replace PACKAGE util AS

    gc_min_salary CONSTANT NUMBER := 2000;
    gc_percent_of_min_salary CONSTANT NUMBER := 1.5;
    TYPE rec_value_list IS RECORD (value_list VARCHAR2(100));
    TYPE tab_value_list IS TABLE OF rec_value_list;
    
    TYPE rec_exchange IS RECORD (r030         NUMBER,
                                 txt          VARCHAR2(100),
                                 rate         NUMBER,
                                 cur          VARCHAR2(100),
                                 exchangedate DATE );
    TYPE tab_exchange IS TABLE OF rec_exchange;
    
    FUNCTION table_from_list(p_list_val IN VARCHAR2,
                             p_separator  IN VARCHAR2 DEFAULT ',') RETURN tab_value_list PIPELINED;
                             
    FUNCTION get_currency(p_currency IN VARCHAR2 DEFAULT 'USD',
                          p_exchangedate IN DATE DEFAULT SYSDATE) RETURN tab_exchange PIPELINED;                             


    FUNCTION add_years(p_date IN DATE DEFAULT SYSDATE,
                       p_year IN NUMBER) RETURN DATE;
                       
    FUNCTION get_sum_price_sales(p_table IN VARCHAR2) RETURN NUMBER;
                       
    PROCEDURE add_new_jobs(p_job_id        IN VARCHAR2, 
                              p_job_title  IN VARCHAR2, 
                              p_min_salary IN NUMBER, 
                              p_max_salary IN NUMBER DEFAULT NULL,
                              po_err       OUT VARCHAR2);

    PROCEDURE del_jobs(p_job_id IN VARCHAR2,
                       po_result OUT VARCHAR2);
                       
    FUNCTION get_dep_name(p_employee_id IN NUMBER) RETURN VARCHAR2;
    
    PROCEDURE update_balance(p_employee_id IN NUMBER,
                             p_balance     IN NUMBER);    
                             
    --Домашнє завдання 7.1
    TYPE rec_empcnt IS RECORD (region_name VARCHAR2(100),
                               emp_count   NUMBER);
    TYPE tab_empcnt IS TABLE OF rec_empcnt;
    
    FUNCTION get_region_cnt_emp(p_department_id IN VARCHAR2 DEFAULT NULL) RETURN tab_empcnt PIPELINED;
    

--==ПРОЕКТ==--

--Додавання співробітника
    PROCEDURE add_employee(p_first_name     IN VARCHAR2,
                           p_last_name      IN VARCHAR2,
                           p_email          IN VARCHAR2,
                           p_phone_number   IN VARCHAR2,
                           p_hire_date      IN DATE DEFAULT TRUNC(SYSDATE, 'dd'),
                           p_job_id         IN VARCHAR2,
                           p_salary         IN NUMBER,
                           p_commission_pct IN NUMBER DEFAULT NULL,
                           p_manager_id     IN NUMBER DEFAULT 100,
                           p_department_id  IN NUMBER);
                           
--Видалення співробітника
    PROCEDURE fire_an_employee(p_employee_id IN NUMBER);
    
--Зміна атрибутів співробітника
PROCEDURE change_attribute_employee(p_employee_id    IN NUMBER,
                                    p_first_name     IN VARCHAR2 DEFAULT NULL,
                                    p_last_name      IN VARCHAR2 DEFAULT NULL,
                                    p_email          IN VARCHAR2 DEFAULT NULL,
                                    p_phone_number   IN VARCHAR2 DEFAULT NULL,
                                    p_job_id         IN VARCHAR2 DEFAULT NULL,
                                    p_salary         IN NUMBER DEFAULT NULL,
                                    p_commission_pct IN NUMBER DEFAULT NULL,
                                    p_manager_id IN  NUMBER DEFAULT NULL,
                                    p_department_id  IN NUMBER DEFAULT NULL);
                            
--Копіювання таблиць
PROCEDURE copy_table(p_source_scheme  IN VARCHAR2,
                     p_target_scheme  IN VARCHAR2 DEFAULT USER,
                     p_list_table     IN VARCHAR2,
                     p_copy_data      IN BOOLEAN DEFAULT FALSE,
                     po_result        OUT VARCHAR2);
                     
--Cинхронізація даних з API
PROCEDURE api_nbu_sync;

END util;

--BODY
create or replace PACKAGE BODY util AS

FUNCTION table_from_list(p_list_val IN VARCHAR2,
                         p_separator  IN VARCHAR2 DEFAULT ',') RETURN tab_value_list PIPELINED IS
                         
    out_rec tab_value_list := tab_value_list(); --ініціалізація змінної
    l_cur   SYS_REFCURSOR;
                         
BEGIN

    OPEN l_cur FOR
    
        SELECT TRIM(REGEXP_SUBSTR(p_list_val, '[^'||p_separator||']+', 1, LEVEL)) AS cur_value
        FROM dual
        CONNECT BY LEVEL <= REGEXP_COUNT(p_list_val, p_separator) + 1;
    
        BEGIN
        
            LOOP
                EXIT WHEN l_cur%NOTFOUND;
                FETCH l_cur BULK COLLECT
                    INTO out_rec;
                    FOR i IN 1..out_rec.count LOOP
                        PIPE ROW(out_rec(i));
                    END LOOP;
            END LOOP;
            CLOSE l_cur;
        
        EXCEPTION 
            WHEN OTHERS THEN
                IF (l_cur%ISOPEN) THEN
                    CLOSE l_cur;
                    RAISE;
                ELSE
                    RAISE;
                END IF;
            
        END;

END table_from_list;


FUNCTION get_currency(p_currency IN VARCHAR2 DEFAULT 'USD',
                      p_exchangedate IN DATE DEFAULT SYSDATE) RETURN tab_exchange PIPELINED IS
                         
    out_rec tab_exchange := tab_exchange(); --ініціалізація змінної
    l_cur   SYS_REFCURSOR;
                         
BEGIN

    OPEN l_cur FOR
    
        SELECT tt.r030, 
               tt.txt, 
               tt.rate, 
               tt.cur, 
               TO_DATE(tt.exchangedate, 'dd.mm.yyyy') AS exchangedate
        FROM (SELECT get_needed_curr(p_valcode => p_currency,p_date => p_exchangedate) AS json_value FROM dual)
        CROSS JOIN json_table
        (
            json_value,'$[*]'
             COLUMNS
            (
             r030         NUMBER        PATH '$.r030',
             txt          VARCHAR2(100) PATH '$.txt',
             rate         NUMBER        PATH '$.rate',
             cur          VARCHAR2(100) PATH '$.cc',
             exchangedate VARCHAR2(100) PATH '$.exchangedate'
            )
        ) tt;

        BEGIN
        
            LOOP
                EXIT WHEN l_cur%NOTFOUND;
                FETCH l_cur BULK COLLECT
                    INTO out_rec;
                    FOR i IN 1..out_rec.count LOOP
                        PIPE ROW(out_rec(i));
                    END LOOP;
            END LOOP;
            CLOSE l_cur;
        
        EXCEPTION 
            WHEN OTHERS THEN
                IF (l_cur%ISOPEN) THEN
                    CLOSE l_cur;
                    RAISE;
                ELSE
                    RAISE;
                END IF;
            
        END;

END get_currency;




--Домашне завдання 4.1
PROCEDURE check_work_time IS

BEGIN

    IF TO_CHAR(SYSDATE, 'DY', 'NLS_DATE_LANGUAGE = AMERICAN') IN ('SAT','SUN') THEN
        raise_application_error(-20205, 'Ви можете вносити зміни лише в робочі дні');
    END IF;
    
END check_work_time;

--Функція
FUNCTION add_years(p_date IN DATE DEFAULT SYSDATE,
                   p_year IN NUMBER) RETURN DATE IS
    v_date DATE;
    v_year NUMBER := p_year*12;

BEGIN

    SELECT add_months(p_date, v_year)
    INTO v_date
    FROM dual;

    RETURN v_date;

END add_years;

--Процедура
PROCEDURE add_new_jobs(p_job_id     IN VARCHAR2, 
                       p_job_title  IN VARCHAR2, 
                       p_min_salary IN NUMBER, 
                       p_max_salary IN NUMBER DEFAULT NULL,
                       po_err       OUT VARCHAR2) IS
                              
    v_max_salary dmitro_h93.jobs.max_salary%TYPE;
    salary_err     EXCEPTION;

BEGIN

    BEGIN
        util.check_work_time;
    END;

    IF p_max_salary IS NULL THEN
        v_max_salary := p_min_salary * gc_percent_of_min_salary;
    ELSE
        v_max_salary := p_max_salary;
    END IF;
    
    BEGIN
    
        IF (p_min_salary < gc_min_salary OR p_max_salary < gc_min_salary) THEN
            raise salary_err;
        ELSE
            INSERT INTO dmitro_h93.jobs (job_id, job_title, min_salary, max_salary)
            VALUES (p_job_id, p_job_title, p_min_salary, v_max_salary); 
            --COMMIT;
            po_err := 'Посада '||p_job_id||' успішно додана';
        END IF;
    EXCEPTION 
        WHEN salary_err THEN 
            raise_application_error(-20001, 'Передана зарплата менше 2000');
        WHEN dup_val_on_index THEN 
            raise_application_error(-20002, 'Посада '||p_job_id||' вже існує');
        WHEN OTHERS THEN
            raise_application_error(-20003, 'Невідома помилка при додаванні нової посади. '||SQLERRM);
    END;
    
    --COMMIT;

END add_new_jobs;

--Домашне завдання
PROCEDURE del_jobs(p_job_id  IN VARCHAR2,
                   po_result OUT VARCHAR2) IS

    v_delete_no_data_found EXCEPTION;
    
BEGIN

    BEGIN
        util.check_work_time;
    END;
    
    BEGIN
        DELETE FROM dmitro_h93.jobs j
        WHERE j.job_id = p_job_id;
        --COMMIT;
        IF SQL%ROWCOUNT = 0 THEN
            RAISE v_delete_no_data_found;
        END IF;        
    EXCEPTION
        WHEN v_delete_no_data_found THEN
            raise_application_error(-20004, 'Посада '||p_job_id||' не існує');
    END;
    
        po_result := 'Посада '||p_job_id||' успішно видалена';

END del_jobs;

--Домашнє завдання
FUNCTION get_dep_name(p_employee_id IN NUMBER) RETURN VARCHAR2 IS
    v_department_name dmitro_h93.departments.department_name%TYPE;

BEGIN

    SELECT de.department_name
    INTO v_department_name
    FROM dmitro_h93.employees em
    LEFT JOIN dmitro_h93.departments de
    ON em.department_id = de.department_id
    WHERE em.employee_id = p_employee_id;

    RETURN v_department_name;

END get_dep_name;


--Модуль 4

PROCEDURE update_balance(p_employee_id IN NUMBER,
                         p_balance     IN NUMBER) IS
                         
         v_balance_new balance.balance%TYPE;
         v_balance_old balance.balance%TYPE;
         v_message     logs.message%TYPE;
                         
BEGIN
    SELECT balance
    INTO v_balance_old
    FROM balance b
    WHERE b.employee_id = p_employee_id
    FOR UPDATE; -- Блокуємо рядок для оновлення
    
    IF v_balance_old >= p_balance THEN
        UPDATE balance b
        SET b.balance = v_balance_old - p_balance
        WHERE employee_id = p_employee_id
        RETURNING b.balance INTO v_balance_new; -- щоб не робити новий SELECT INTO
    ELSE
        v_message := 'Employee_id = '||p_employee_id||'. Недостатньо коштів на рахунку. Поточний баланс '||v_balance_old||', спроба зняття '||p_balance||'';
        raise_application_error(-20001, v_message);
    END IF;
    
    v_message := 'Employee_id = '||p_employee_id||'. Кошти успішно зняті з рахунку. Було '||v_balance_old||', стало '||v_balance_new||'';
    dbms_output.put_line(v_message);
    to_log(p_appl_proc => 'util.update_balance', p_message => v_message);
    
    /*IF 1=0 THEN -- зімітуємо непередбачену помилку
        v_message := 'Непередбачена помилка';
        raise_application_error(-20001, v_message);
    END IF;*/
    
    COMMIT; -- зберігаємо новий баланс та знімаємо блокування в поточній транзакції
    
    EXCEPTION
        WHEN OTHERS THEN
            to_log(p_appl_proc => 'util.update_balance', p_message => NVL(v_message, 'Employee_id = '||p_employee_id||'. ' ||SQLERRM));
        ROLLBACK; -- Відміняємо транзакцію у разі виникнення помилки
            raise_application_error(-20001, NVL(v_message, 'Не відома помилка'));
            
END update_balance;

--Домашнє завдання 4.3
FUNCTION get_sum_price_sales(p_table IN VARCHAR2) RETURN NUMBER IS

    v_sum_price_sales NUMBER;
    v_dynamic_sql VARCHAR2(100);
    v_table VARCHAR2(30) := p_table;
    v_masage VARCHAR2(100);

BEGIN

    IF v_table NOT IN ('products','products_old') THEN

        v_masage := 'Неприпустиме значення! Очікується products або products_old';
        to_log(p_appl_proc => 'util.get_sum_price_sales', p_message => v_masage);
        raise_application_error(-20001, v_masage);

    END IF;
    
     v_dynamic_sql :='
        SELECT SUM(p.price_sales)
        FROM hr.'||p_table||' p';
        
    EXECUTE IMMEDIATE v_dynamic_sql INTO v_sum_price_sales;

    RETURN v_sum_price_sales;

END get_sum_price_sales;

--Домашнє завдання 7.1
FUNCTION get_region_cnt_emp(p_department_id IN VARCHAR2 DEFAULT NULL) RETURN tab_empcnt PIPELINED IS
                         
    out_rec tab_empcnt := tab_empcnt(); --ініціалізація змінної
    l_ctn   SYS_REFCURSOR;
                         
BEGIN

    OPEN l_ctn FOR
    
        SELECT reg.region_name, count (em.employee_id) AS emp_count
            FROM hr.regions reg
                LEFT JOIN hr.countries con ON reg.region_id = con.region_id
                LEFT JOIN hr.locations loc ON con.country_id = loc.country_id
                LEFT JOIN hr.departments dep ON loc.location_id = dep.location_id
                LEFT JOIN hr.employees em ON dep.department_id = em.department_id
            WHERE (em.department_id = p_department_id or p_department_id is null)
            GROUP BY reg.region_name;

        BEGIN
        
            LOOP
                EXIT WHEN l_ctn%NOTFOUND;
                FETCH l_ctn BULK COLLECT
                    INTO out_rec;
                    FOR i IN 1..out_rec.count LOOP
                        PIPE ROW(out_rec(i));
                    END LOOP;
            END LOOP;
            CLOSE l_ctn;
        
        EXCEPTION 
            WHEN OTHERS THEN
                IF (l_ctn%ISOPEN) THEN
                    CLOSE l_ctn;
                    RAISE;
                ELSE
                    RAISE;
                END IF;
            
        END;

END get_region_cnt_emp;


--==ПРОЕКТ==--

--Перевірка "бізнес час"
PROCEDURE check_biz_time IS

BEGIN

    IF TO_CHAR(SYSDATE, 'DY', 'NLS_DATE_LANGUAGE = AMERICAN') IN ('SAT','SUN') OR
       TO_CHAR(SYSDATE, 'hh24mi') BETWEEN '1801' AND '2359' OR 
       TO_CHAR(SYSDATE, 'hh24mi') BETWEEN '0000' AND '0759' THEN
        raise_application_error(-20001, 'Ви можете видаляти співробітника лише в робочий час');
    END IF;
    
END check_biz_time;

--Додавання співробітника
PROCEDURE add_employee(p_first_name     IN VARCHAR2,
                       p_last_name      IN VARCHAR2,
                       p_email          IN VARCHAR2,
                       p_phone_number   IN VARCHAR2,
                       p_hire_date      IN DATE DEFAULT TRUNC(SYSDATE, 'dd'),
                       p_job_id         IN VARCHAR2,
                       p_salary         IN NUMBER,
                       p_commission_pct IN NUMBER DEFAULT NULL,
                       p_manager_id     IN NUMBER DEFAULT 100,
                       p_department_id  IN NUMBER) IS
                       
    v_exist_jobid NUMBER;
    v_exist_depid NUMBER;
    v_sal_range   NUMBER;
    v_maxid       NUMBER;
                       
BEGIN

    log_util.log_start('add_employee');
    
    SELECT COUNT(*)
    INTO v_exist_jobid
    FROM dmitro_h93.jobs jo
    WHERE jo.job_id = p_job_id;
    
    IF v_exist_jobid = 0 THEN
        raise_application_error(-20001,'Введено неіснуючий код посади');
    END IF;
    
    SELECT COUNT(*)
    INTO v_exist_depid
    FROM dmitro_h93.departments dep
    WHERE dep.department_id = p_department_id;
    
    IF v_exist_depid = 0 THEN
        raise_application_error(-20001,'Введено неіснуючий ідентифікатор відділу');
    END IF;
    
    SELECT COUNT(*)
    INTO v_sal_range
    FROM dmitro_h93.jobs jo
    WHERE jo.job_id = p_job_id
    AND p_salary BETWEEN jo.min_salary AND jo.max_salary;
    
    IF v_sal_range = 0 THEN
        raise_application_error(-20001,'Введено неприпустиму заробітну плату для даного коду посади');
    END IF;
    
    check_biz_time;
  
    SELECT NVL(MAX(emp.employee_id),0)+1
    INTO v_maxid
    FROM dmitro_h93.employees emp;
    
    BEGIN    

        INSERT INTO dmitro_h93.employees (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
        VALUES (v_maxid, p_first_name, p_last_name, p_email, p_phone_number, p_hire_date, p_job_id, p_salary, p_commission_pct, p_manager_id, p_department_id); 
        
        COMMIT;
    
        dbms_output.put_line('Співробітник '||p_first_name||' '||p_last_name||' '||p_job_id||' '||p_department_id||' успішно додано до системи');
        
        EXCEPTION
            WHEN OTHERS THEN
                log_util.log_error('add_employee', sqlerrm);
        
    END;
    
    log_util.log_finish('add_employee');

END add_employee;

--Видалення співробітника
PROCEDURE fire_an_employee(p_employee_id IN NUMBER) IS

    v_first_name    VARCHAR2(50);
    v_last_name     VARCHAR2(50);
    v_job_id        VARCHAR2(30);
    v_department_id NUMBER;
    v_hire_date     DATE;

BEGIN

    log_util.log_start('fire_an_employee');
    
    check_biz_time;
    
    BEGIN
        
        SELECT emp.first_name, emp.last_name, emp.job_id, emp.department_id, emp.hire_date
        INTO v_first_name, v_last_name, v_job_id, v_department_id, v_hire_date
        FROM dmitro_h93.employees emp
        WHERE emp.employee_id = p_employee_id;
    
        INSERT INTO dmitro_h93.employees_history (employee_id, first_name, last_name, job_id, department_id, hire_date, termination_date)
        VALUES (p_employee_id, v_first_name, v_last_name, v_job_id, v_department_id, v_hire_date, SYSDATE);
            
        DELETE FROM dmitro_h93.employees emp
        WHERE emp.employee_id = p_employee_id;
        
        COMMIT;
        
        dbms_output.put_line('Співробітник '||v_first_name||' '||v_last_name||' '||v_job_id||' '||v_department_id||' успішно видалено з системи');
        
        EXCEPTION
            WHEN no_data_found THEN
                raise_application_error(-20001,'Переданий співробітник не існує');
            WHEN OTHERS THEN
                log_util.log_error('fire_an_employee', sqlerrm);
     
    END;
    
    log_util.log_finish('fire_an_employee');

END fire_an_employee;

--Зміна атрибутів співробітника
PROCEDURE change_attribute_employee(p_employee_id    IN NUMBER,
                                    p_first_name     IN VARCHAR2 DEFAULT NULL,
                                    p_last_name      IN VARCHAR2 DEFAULT NULL,
                                    p_email          IN VARCHAR2 DEFAULT NULL,
                                    p_phone_number   IN VARCHAR2 DEFAULT NULL,
                                    p_job_id         IN VARCHAR2 DEFAULT NULL,
                                    p_salary         IN NUMBER DEFAULT NULL,
                                    p_commission_pct IN NUMBER DEFAULT NULL,
                                    p_manager_id IN  NUMBER DEFAULT NULL,
                                    p_department_id  IN NUMBER DEFAULT NULL) IS
                                    
    v_dynamic_sql VARCHAR2(4000);
    v_set_string VARCHAR2(4000) := '';
                                    
BEGIN

    log_util.log_start('change_attribute_employee');
    
    IF p_first_name IS NULL AND 
       p_last_name IS NULL AND 
       p_email IS NULL AND 
       p_phone_number IS NULL AND 
       p_job_id IS NULL AND 
       p_salary IS NULL AND 
       p_commission_pct IS NULL AND 
       p_manager_id IS NULL AND 
       p_department_id IS NULL THEN
          log_util.log_finish('change_attribute_employee'); 
          raise_application_error(-20001, 'Вкажіть хоча б один параметр для оновлення');                        
    END IF;
    
    IF p_first_name IS NOT NULL THEN
        v_set_string := v_set_string || 'emp.first_name = ''' || p_first_name || ''', ';
    END IF;
        
    IF p_last_name IS NOT NULL THEN
        v_set_string := v_set_string || 'emp.last_name = ''' || p_last_name || ''', ';
    END IF;
        
    IF p_email IS NOT NULL THEN
        v_set_string := v_set_string || 'emp.email = ''' || p_email || ''', ';
    END IF;
        
    IF p_phone_number IS NOT NULL THEN
        v_set_string := v_set_string || 'emp.phone_number = ''' || p_phone_number || ''', ';
    END IF;
        
    IF p_job_id IS NOT NULL THEN
        v_set_string := v_set_string || 'emp.job_id = ''' || p_job_id || ''', ';
    END IF;
        
    IF p_salary IS NOT NULL THEN
        v_set_string := v_set_string || 'emp.salary = ' || p_salary || ', ';
    END IF;
        
    IF p_commission_pct IS NOT NULL THEN
        v_set_string := v_set_string || 'emp.commission_pct = ' || p_commission_pct || ', ';
    END IF;
        
    IF p_manager_id IS NOT NULL THEN
        v_set_string := v_set_string || 'emp.manager_id = ' || p_manager_id || ', ';
    END IF;
        
    IF p_department_id IS NOT NULL THEN
        v_set_string := v_set_string || 'emp.department_id = ' || p_department_id || ', ';        
    END IF;   
    
    v_set_string := RTRIM(v_set_string, ', ');
    
    v_dynamic_sql := 'UPDATE dmitro_h93.employees emp 
                      SET ' ||v_set_string|| ' 
                      WHERE emp.employee_id = ' ||p_employee_id;
                                   
    BEGIN
        EXECUTE IMMEDIATE v_dynamic_sql;
        
        COMMIT;
        
        dbms_output.put_line('У співробітника '||p_employee_id||' успішно оновлені атрибути');
        
        EXCEPTION
            WHEN OTHERS THEN
                log_util.log_error('change_attribute_employee', sqlerrm);
    END;
    
    log_util.log_finish('change_attribute_employee');
    
END change_attribute_employee;

--Копіювання таблиць
PROCEDURE copy_table(p_source_scheme  IN VARCHAR2,
                     p_target_scheme  IN VARCHAR2 DEFAULT USER,
                     p_list_table     IN VARCHAR2,
                     p_copy_data      IN BOOLEAN DEFAULT FALSE,
                     po_result        OUT VARCHAR2) IS
                     
    v_dynsql_create VARCHAR2(4000);  
    v_dynsql_copy   VARCHAR2(4000);  
    v_table_name    VARCHAR2(100); 
    
BEGIN

    to_log('copy_table', 'Початок копіювання таблиць '||p_list_table||' з '|| p_source_scheme ||' до '|| p_target_scheme);

        FOR cc IN (
            SELECT table_name, 
                   'CREATE TABLE ' || p_target_scheme || '.' || table_name || ' (' ||
                   LISTAGG(column_name || ' ' || data_type || count_symbol, ', ') WITHIN GROUP(ORDER BY column_id) || ')' AS ddl_code
            FROM (
                SELECT table_name,
                       column_name,
                       data_type,
                       CASE 
                           WHEN data_type IN ('VARCHAR2', 'CHAR') THEN '(' || data_length || ')'
                           WHEN data_type = 'DATE' THEN NULL 
                           WHEN data_type = 'NUMBER' THEN REPLACE('(' || data_precision || ',' || data_scale || ')', '(,)', NULL)
                       END AS count_symbol,
                       column_id
                FROM all_tab_columns
                WHERE owner = p_source_scheme
                  AND table_name IN (SELECT * FROM TABLE(table_from_list(p_list_table))) 
                  AND table_name NOT IN (SELECT table_name FROM all_tables WHERE owner = p_target_scheme)
                ORDER BY table_name, column_id
            )
            GROUP BY table_name
        ) LOOP
    
        BEGIN
        
        v_table_name := cc.table_name;
        v_dynsql_create := cc.ddl_code;

        to_log('copy_table', 'Обробка таблиці: ' || v_table_name); 
        
        EXECUTE IMMEDIATE v_dynsql_create;
        to_log('copy_table', 'Таблицю ' || v_table_name || ' успішно створено в схемі ' || p_target_scheme);

            IF p_copy_data = TRUE THEN
                v_dynsql_copy := 'INSERT INTO ' || p_target_scheme || '.' || v_table_name || 
                                 ' SELECT * FROM ' || p_source_scheme || '.' || v_table_name;
                EXECUTE IMMEDIATE v_dynsql_copy;
                to_log('copy_table', 'Дані з таблиці ' || p_source_scheme || '.' || v_table_name || ' успішно скопійовані в ' || p_target_scheme || '.' || v_table_name);
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                to_log('copy_table', 'Помилка під час копіювання таблиці ' || v_table_name || ': ' || sqlerrm);
                CONTINUE;
        END;
    END LOOP;

    to_log('copy_table', 'Копіювання таблиць '||p_list_table||' з '||p_source_scheme||' до '||p_target_scheme||' завершено');
    po_result := 'Таблиці '||p_list_table||' успішно скопійовані з '||p_source_scheme||' до '||p_target_scheme;
    
    EXCEPTION
        WHEN OTHERS THEN
            to_log('copy_table', 'Помилка під час копіювання таблиць: ' || sqlerrm);
            po_result := 'Помилка під час копіювання таблиць: ' || sqlerrm;
            
END copy_table;

--Cинхронізація даних з API
PROCEDURE api_nbu_sync IS
                      
    v_list_currencies VARCHAR2(2000);
                       
BEGIN
    
    log_util.log_start('api_nbu_sync');
    
    BEGIN
    
        SELECT value_text
        INTO v_list_currencies
        FROM sys_params
        WHERE param_name = 'list_currencies';
        
    EXCEPTION
        WHEN OTHERS THEN
            log_util.log_error('api_nbu_sync', sqlerrm);
            raise_application_error(-20001, 'Виникла помилка: '||sqlerrm);
            
    END;
    
    FOR cc IN (SELECT value_list AS curr 
               FROM TABLE(util.table_from_list(p_list_val => v_list_currencies))) LOOP
               
    INSERT INTO cur_exchange (r030, txt, rate, cur, exchangedate)
    SELECT tt.r030, 
           tt.txt, 
           tt.rate, 
           tt.cur, 
           tt.exchangedate
    FROM TABLE(util.get_currency(p_currency => cc.curr)) tt;
    
    END LOOP;
    
    log_util.log_finish('api_nbu_sync');
    
END api_nbu_sync;

END util;
