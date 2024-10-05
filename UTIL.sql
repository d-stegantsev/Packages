--Специфікація
create or replace PACKAGE util AS
--==ПРОЕКТ==--
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
                            

END util;

--BODY
create or replace PACKAGE BODY util AS
--==ПРОЕКТ==--
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
        
        IF TO_CHAR(SYSDATE, 'DY', 'NLS_DATE_LANGUAGE = AMERICAN') IN ('SAT','SUN') OR
           TO_CHAR(SYSDATE, 'hh24mi') BETWEEN '1801' AND '2359' OR 
           TO_CHAR(SYSDATE, 'hh24mi') BETWEEN '0000' AND '0759' THEN
            raise_application_error(-20001, 'Ви можете додавати нового співробітника лише в робочий час');
        END IF;
      
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


END util;
