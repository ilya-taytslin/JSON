create or replace PROCEDURE json_treat_test (v_input  IN VARCHAR2)
    IS
    
    v_result clob := '';
    ja JSON_ARRAY_T;
    jo JSON_OBJECT_T;
    je JSON_ELEMENT_T;
    keys        JSON_KEY_LIST;
    keys_string VARCHAR2(100);
    v_filetype VARCHAR2(100);

begin
  ja := new JSON_ARRAY_T;
  je :=  JSON_ELEMENT_T.parse(lower(v_input));
  IF (je.is_Object) THEN
      jo := treat(je AS JSON_OBJECT_T);
      keys := jo.get_keys;
      FOR i IN 1..keys.COUNT LOOP
         ja.append(keys(i));
      END LOOP;
      keys_string := ja.to_string;
      --DBMS_OUTPUT.put_line(keys_string);
      DBMS_OUTPUT.put_line(keys(1));
      DBMS_OUTPUT.put_line(keys(2)); /* Generates error if only one key */
      IF (keys(1) = 'filetype') THEN
        v_filetype := jo.get_string('filetype');
        DBMS_OUTPUT.put_line(v_filetype);
      ELSE
        DBMS_OUTPUT.put_line('First JSON element is not "filetype"');
      END IF;
  ELSE
    DBMS_OUTPUT.put_line('Not a JSON');
  END IF;
 
end;