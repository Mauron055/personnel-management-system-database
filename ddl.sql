--Процедура, которая обновляет почасовую ставку сотрудников на определённый процент. При понижении ставка не может быть ниже минимальной — 500 рублей в час. Если по расчётам выходит меньше, устанавливают минимальную ставку.
CREATE OR REPLACE PROCEDURE update_employees_rate(p_employee json)
LANGUAGE plpgsql
AS $$
DECLARE
    emp_record json;
BEGIN
    FOR emp_record IN SELECT * FROM json_array_elements(p_employee)
    LOOP
        UPDATE employees
        SET rate = 
            CASE 
                WHEN rate + (rate * (emp_record->>'rate_change')::float / 100) < 500 
                    THEN 500 
                ELSE rate + (rate * (emp_record->>'rate_change')::float / 100) 
            END
        WHERE id = (emp_record->>'employee_id')::uuid;
    END LOOP;
END;
$$
--Хранимую процедура, которая повышает зарплаты всех сотрудников на определённый процент. 
--Процедура принимает один целочисленный параметр — процент индексации p. Сотрудникам, которые получают зарплату по ставке ниже средней относительно всех сотрудников до индексации, начисляют дополнительные 2% (p + 2). Ставка остальных сотрудников увеличивается на p%.
CREATE OR REPLACE PROCEDURE indexing_salary(p_p integer)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE employees
    SET rate = 
        CASE 
            WHEN rate < (SELECT AVG(rate) FROM employees)
                THEN rate + (rate * (p_p + 2) / 100)
            ELSE rate + (rate * p_p / 100) 
        END;
END;
$$
--Пользовательская процедура завершения проекта.
CREATE OR REPLACE PROCEDURE close_project(p_project_id uuid)
LANGUAGE plpgsql
AS $$
DECLARE
  _hours_all integer;
  _hours_max integer;
  _workers integer;
  _bonus integer;
  _activity boolean;
BEGIN

  SELECT is_active
  INTO _activity
  FROM projects
  WHERE id = p_project_id;

  -- Проверяем, активен ли проект перед обновлением
  IF _activity = true THEN
    UPDATE projects
    SET is_active = false
    WHERE id = p_project_id AND is_active = true;

    SELECT SUM(work_hours)
    INTO _hours_all
    FROM logs
    WHERE project_id = p_project_id;

    SELECT estimated_time
    INTO _hours_max
    FROM projects
    WHERE id = p_project_id;

    SELECT COUNT(DISTINCT employee_id)
    INTO _workers
    FROM logs
    WHERE project_id = p_project_id;

    IF _hours_all < _hours_max AND _hours_max IS NOT NULL THEN
      _bonus := FLOOR((_hours_max - _hours_all) * 0.75 / _workers);
      IF _bonus > 16 THEN
        _bonus := 16;
      END IF;

      -- Обновление таблицы logs с бонусными часами
      UPDATE logs
      SET work_hours = work_hours + _bonus,
          created_at = current_timestamp
      WHERE project_id = p_project_id;
    END IF;
  ELSE
    RAISE NOTICE 'Проект уже закрыт';
    RETURN;
  END IF;

END;
$$
--Процедура для внесения отработанных сотрудниками часов. Процедура добавляет новые записи о работе сотрудников над проектами.
CREATE OR REPLACE PROCEDURE log_work(p_employee_uuid uuid, p_project_uuid uuid, p_work_date date, p_hours int)
LANGUAGE plpgsql
AS $$
DECLARE
	_activity boolean;
BEGIN
	SELECT is_active
	INTO _activity
	FROM projects
	WHERE id = p_project_uuid;
	
	IF NOT _activity THEN
		RAISE NOTICE 'Project closed';
	ELSIF p_hours > 24 OR p_hours < 1 THEN
		RAISE NOTICE 'Недопустимые данные';
	ELSE
    	IF p_hours > 16  OR p_work_date > current_date OR p_work_date < current_date - interval '1 week' THEN
    		INSERT INTO logs (employee_id, project_id, work_date, work_hours, required_review)
    		VALUES (p_employee_uuid, p_project_uuid, p_work_date, p_hours, true);
		ELSE
    		INSERT INTO logs (employee_id, project_id, work_date, work_hours)
    		VALUES (p_employee_uuid, p_project_uuid, p_work_date, p_hours);
  		END IF;
	END IF;
END;
$$
--При добавлении сотрудника в таблицу employees и изменении ставки сотрудника триггер автоматически вносит запись в таблицу employee_rate_history из трёх полей: id сотрудника, его ставки и текущей даты.
CREATE TABLE employee_rate_history
(
	id SERIAL,
	employee_id UUID REFERENCES employees,
	rate INT,
	from_date DATE DEFAULT CURRENT_DATE
);

INSERT INTO employee_rate_history (employee_id, rate, from_date)
SELECT id, rate, '2020-12-26'::date
FROM employees;

CREATE OR REPLACE FUNCTION save_employee_rate_history()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN

    INSERT INTO employee_rate_history (employee_id, rate)
    VALUES (NEW.id, NEW.rate);
    RETURN NEW;
	
END;
$$;

CREATE OR REPLACE TRIGGER change_employee_rate
AFTER INSERT OR UPDATE ON employees
FOR EACH ROW
EXECUTE FUNCTION save_employee_rate_history();
--Функция принимает id проекта и возвращает таблицу с именами трёх сотрудников, которые залогировали максимальное количество часов в этом проекте. Результирующая таблица состоит из двух полей: имени сотрудника и количества часов, отработанных на проекте.
CREATE OR REPLACE FUNCTION best_project_workers(p_uuid uuid)
RETURNS TABLE (employee text, work_hours bigint)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT e.name as employee, SUM(l.work_hours) as work_hours
	FROM logs l JOIN employees e ON l.employee_id = e.id
    WHERE p_uuid = l.project_id
	GROUP BY e.name
	ORDER BY work_hours DESC
	LIMIT 3;
END;
$$
--Функция для расчёта зарплаты за месяц.
CREATE OR REPLACE FUNCTION calculate_month_salary(p_start_date date, p_end_date date)
RETURNS TABLE (id uuid, worked_hours int, salary int)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT l.employee_id as id, SUM(l.work_hours)::integer as worked_hours, (SUM(l.work_hours) * e.rate)::integer as salary
	FROM logs l JOIN employees e ON e.id = l.employee_id
	WHERE l.required_review = false AND l.is_paid = false AND l.work_date >= p_start_date AND l.work_date <= p_end_date
	GROUP BY l.employee_id, e.rate;
END;
$$
