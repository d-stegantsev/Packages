--Специфікація
create or replace PACKAGE util AS
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
                                        p_manager_id     IN NUMBER DEFAULT NULL,
                                        p_department_id  IN NUMBER DEFAULT NULL);
                            
END util;

--BODY
create or replace PACKAGE BODY util AS
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

    v_exist_empid   NUMBER;
    v_first_name    VARCHAR2(50);
    v_last_name     VARCHAR2(50);
    v_job_id        VARCHAR2(30);
    v_department_id NUMBER;
    v_hire_date     DATE;

BEGIN

    log_util.log_start('fire_an_employee');
    
    SELECT COUNT(*)
    INTO v_exist_empid
    FROM dmitro_h93.employees emp
    WHERE emp.employee_id = p_employee_id;
    
    IF v_exist_empid = 0 THEN
        raise_application_error(-20001,'Переданий співробітник не існує');
    END IF;
    
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


END util;
