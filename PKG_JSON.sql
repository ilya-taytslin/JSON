create or replace package PKG_JSON IS

    -- Public Package Constants
    
    VERBOSE_DEBUG_LEVEL             CONSTANT SMALLINT := 5;
    PERFORMANCE_METRICS_DEBUG_LEVEL CONSTANT SMALLINT := 4;
    INFORMATIONAL_DEBUG_LEVEL       CONSTANT SMALLINT := 3;
    WARNING_DEBUG_LEVEL             CONSTANT SMALLINT := 2;
    ERROR_DEBUG_LEVEL               CONSTANT SMALLINT := 1;
    OFF_DEBUG_LEVEL                 CONSTANT SMALLINT := 0;
    
    -- End Public Package Constants
    
    --SET debug level to check what is happening
    PROCEDURE SET_SESSION_DEBUG_LEVEL( asi_DebugLevel  IN SMALLINT );
    
    --Validate the input JSON if appropriate
    PROCEDURE validate_input (v_input IN VARCHAR2, v_filecode OUT NUMBER, v_timestamp OUT NUMBER);

    -- MAIN procedure for generating chart area JSON via NOAA schema lookup
    PROCEDURE chart_area_json (v_result OUT CLOB );
    
    -- Procedure for generating chart area JSON via VTR schema lookup
    PROCEDURE chart_area_vtr_json (v_result OUT CLOB );
    
    -- MAIN procedure for generating gear JSON 
    PROCEDURE gear_json (v_result OUT CLOB );
    
    -- MAIN procedure for generating dealer JSON 
    PROCEDURE dealer_json (v_input IN CLOB, v_result OUT CLOB );
    
    -- Procedure for generating JIRA dealer JSON 
    PROCEDURE dealer_jira_json (v_result OUT CLOB );    

    -- Procedure for generating JIRA vessel JSON 
    PROCEDURE vessel_jira_json (v_result OUT CLOB );

    -- MAIN procedure for generating operator JSON 
    PROCEDURE operator_json (v_input IN CLOB, v_result OUT CLOB );
    
END PKG_JSON;
/********************************************************************************/

create or replace PACKAGE BODY PKG_JSON AS

    isi_CurrentDebugLevel SMALLINT := OFF_DEBUG_LEVEL;

    PROCEDURE SET_SESSION_DEBUG_LEVEL( asi_DebugLevel  IN SMALLINT )
    IS 
        lExcpt_InvalidLoggingLevel EXCEPTION;
        ls_MethodName              VARCHAR2(128);
    BEGIN
    
        ls_MethodName := 'SET_SESSION_DEBUG_LEVEL';
    
        If ( ( asi_DebugLevel != OFF_DEBUG_LEVEL ) AND
             ( asi_DebugLevel != ERROR_DEBUG_LEVEL ) AND
             ( asi_DebugLevel != WARNING_DEBUG_LEVEL ) AND
             ( asi_DebugLevel != INFORMATIONAL_DEBUG_LEVEL ) AND
             ( asi_DebugLevel != PERFORMANCE_METRICS_DEBUG_LEVEL ) AND
             ( asi_DebugLevel != VERBOSE_DEBUG_LEVEL ) ) Then
    
            RAISE lExcpt_InvalidLoggingLevel;
        Else
            isi_CurrentDebugLevel := asi_DebugLevel;
        End If;
    
    END SET_SESSION_DEBUG_LEVEL;
/****************************************************************************************************/   
PROCEDURE validate_input (v_input IN VARCHAR2, v_filecode OUT NUMBER, v_timestamp OUT NUMBER)
      --  v_input       CLOB typecast to VARCHAR2, presumably a short JSON file
      --  v_filecode    1: full, 2: incremental, 0: invalid
      --  v_timestamp   Must be a 10 digit number  
   IS
    ja JSON_ARRAY_T;
    jo JSON_OBJECT_T;
    je JSON_ELEMENT_T;
    keys        JSON_KEY_LIST;
    keys_string VARCHAR2(100);
    v_filetype VARCHAR2(100);
    v_timestring VARCHAR2(10);

begin
  v_filecode := 0;
  v_timestamp := 0;
  ja := new JSON_ARRAY_T;
  IF (v_input is json (STRICT)) THEN
    je :=  JSON_ELEMENT_T.parse(lower(v_input));  /* JSON operations are case-sensitive */
  ELSE
    DBMS_OUTPUT.put_line('Not a JSON string');
        return;
  END IF;
  
  IF (je.is_Object) THEN
      jo := treat(je AS JSON_OBJECT_T);
      keys := jo.get_keys;
      IF (keys.count < 1) THEN
        DBMS_OUTPUT.put_line('Empty JSON');
        return;
      END IF;

      IF (keys(1) = 'filetype') THEN
        v_filetype := jo.get_string('filetype');
        DBMS_OUTPUT.put_line(v_filetype);
      ELSE
        DBMS_OUTPUT.put_line('First JSON element is not "filetype"');
        return;
      END IF;
      
      IF (v_filetype = 'full') THEN     -- First JSON element must be either 
        v_filecode := 1;                  -- "filetype":"full" or "filetype":"incremental" 
        return;
      ELSIF (v_filetype <> 'incremental') THEN
        DBMS_OUTPUT.put_line('Invalid filetype');
        return;
      END IF;
      
      IF (keys.count < 2) THEN      -- We get this far only with "filetype":"incremental" 
        DBMS_OUTPUT.put_line('Timestamp not specified');
        return;
      END IF;
      
      IF (keys(2) = 'timestamp') THEN
        v_timestring := substr(jo.get_string('timestamp'),1,10);
        DBMS_OUTPUT.put_line(v_timestring);
      ELSE
        DBMS_OUTPUT.put_line('Second JSON element is not "timestamp"');
        return;
      END IF;
      
      IF (length(v_timestring) < 10 OR VALIDATE_CONVERSION(v_timestring AS NUMBER) = 0) THEN
        DBMS_OUTPUT.put_line('Not a valid timestamp');
        return;
      END IF;
      
      v_filecode := 2;
      v_timestamp := TO_NUMBER(v_timestring);
  ELSE
    v_filecode := 0;
    DBMS_OUTPUT.put_line('Not a JSON');
  END IF;
END ;
/****************************************************************************************************/   
 procedure chart_area_json (v_result OUT clob )
  is

   AREA_CODE CONSTANT VARCHAR2(9) := 'AREA_CODE';
  vcronjob                           VARCHAR2(150)  := 'ON DEMAND';
 	    vbatchprocess                  VARCHAR2 (150)  := NULL;
	    vmodulename                    varchar2 (150)  := 'VTR_JSON'; 
		vprocedurename                 VARCHAR2 (255)  := 'CHART_AREA_JSON';
		vtablename                     VARCHAR2 (50)   := '';	    
		ilogid                         INT             := 0;
		VSQL                           varchar2 (4000);		
        errmsg           VARCHAR2 (2000);
        sql_stmt             VARCHAR2 (2000);
        vowner           VARCHAR2 (50)   :='CFDRS';
        ifsoseq          NUMBER          :=0;
        v_compliance_start  date;
        v_compliance_end    date;
        v_name             VARCHAR2 (25);
        v_debug  NUMBER;
        v_loop  NUMBER;
        v_maxdate date;
        
        area_object  JSON_OBJECT_T := json_object_t();
        area_array  JSON_ARRAY_T := json_array_t();
        empty_array  JSON_ARRAY_T := json_array_t();
  begin
    DBMS_OUTPUT.ENABLE(1000000);
    
    for a_data in (with area_data as (select distinct nemarea as area_code
        FROM noaa.loc2areas
        WHERE NOT REGEXP_LIKE (nemarea, '[0][0-4][0-9]$|[0][5][0]$')
        ORDER BY nemarea ASC)
    select a.area_code
    from area_data a) loop
    
        area_object.put(AREA_CODE, a_data.area_code);
        
         IF ( isi_CurrentDebugLevel >= VERBOSE_DEBUG_LEVEL ) THEN
            DBMS_OUTPUT.PUT_LINE('Area code:' || area_object.to_clob());
         END IF;
    
        area_array.append(area_object);
        --Start cleaning JSON object
        area_object.remove(AREA_CODE);
    end loop;
    
     IF ( isi_CurrentDebugLevel >= ERROR_DEBUG_LEVEL ) THEN
        DBMS_OUTPUT.PUT_LINE('Area JSON:' || area_array.to_clob());
     END IF;
    
    v_result := area_array.to_clob();

  --fso_admin.log_event (vbatchprocess, vmodulename, vprocedurename, ifsoseq, 'SUCCESSFUL', 'Successfully finished procedure.' ,vtablename,NULL,NULL,NULL, ilogid);
  EXCEPTION
    WHEN OTHERS THEN
        errmsg := errmsg || ' SQL Error on ' || vtablename ||' : ' || SQLERRM;
        
        v_result := empty_array.to_clob();
      --  set_run_status(ifsoseq, 'ABORT', -1, errmsg);
        --fso_admin.log_event ( vbatchprocess, vmodulename, vprocedurename, ifsoseq,'FAILED', 'Finished abnormally - '||errmsg,NULL,NULL,NULL,NULL, ilogid );
        DBMS_OUTPUT.PUT_LINE(VPROCEDURENAME || 'finished abnormally'||ERRMSG);
  end;
/****************************************************************************************************/    
 procedure chart_area_vtr_json (v_result OUT clob )
  is

   AREA_CODE CONSTANT VARCHAR2(9) := 'AREA_CODE';
  vcronjob                           VARCHAR2(150)  := 'ON DEMAND';
 	    vbatchprocess                  VARCHAR2 (150)  := NULL;
	    vmodulename                    varchar2 (150)  := 'VTR_JSON'; 
		vprocedurename                 VARCHAR2 (255)  := 'CHART_AREA_VTR_JSON';
		vtablename                     VARCHAR2 (50)   := '';	    
		ilogid                         INT             := 0;
		VSQL                           varchar2 (4000);		
        errmsg           VARCHAR2 (2000);
        sql_stmt             VARCHAR2 (2000);
        vowner           VARCHAR2 (50)   :='CFDRS';
        ifsoseq          NUMBER          :=0;
        v_compliance_start  date;
        v_compliance_end    date;
        v_name             VARCHAR2 (25);
        v_debug  NUMBER;
        v_loop  NUMBER;
        v_maxdate date;
        
        area_object  JSON_OBJECT_T := json_object_t();
        area_array  JSON_ARRAY_T := json_array_t();
        empty_array  JSON_ARRAY_T := json_array_t();
  begin
    DBMS_OUTPUT.ENABLE(1000000);
    
    for a_data in (with area_data as (SELECT distinct nemarea as area_code
        FROM vtr.area ORDER BY nemarea ASC)
    select a.area_code
    from area_data a) loop
    
        area_object.put(AREA_CODE, a_data.area_code);
        
         IF ( isi_CurrentDebugLevel >= VERBOSE_DEBUG_LEVEL ) THEN
            DBMS_OUTPUT.PUT_LINE('Area code:' || area_object.to_clob());
         END IF;
    
        area_array.append(area_object);
        --Start cleaning JSON object
        area_object.remove(AREA_CODE);
    end loop;
    
     IF ( isi_CurrentDebugLevel >= ERROR_DEBUG_LEVEL ) THEN
        DBMS_OUTPUT.PUT_LINE('Area JSON:' || area_array.to_clob());
     END IF;
    
    v_result := area_array.to_clob();

  --fso_admin.log_event (vbatchprocess, vmodulename, vprocedurename, ifsoseq, 'SUCCESSFUL', 'Successfully finished procedure.' ,vtablename,NULL,NULL,NULL, ilogid);
  EXCEPTION
    WHEN OTHERS THEN
        errmsg := errmsg || ' SQL Error on ' || vtablename ||' : ' || SQLERRM;
        
        v_result := empty_array.to_clob();
      --  set_run_status(ifsoseq, 'ABORT', -1, errmsg);
        --fso_admin.log_event ( vbatchprocess, vmodulename, vprocedurename, ifsoseq,'FAILED', 'Finished abnormally - '||errmsg,NULL,NULL,NULL,NULL, ilogid );
        DBMS_OUTPUT.PUT_LINE(VPROCEDURENAME || 'finished abnormally'||ERRMSG);
  end;
/****************************************************************************************************/
 procedure gear_json (v_result OUT clob )
  is
        
        vcronjob                       VARCHAR2(150)   := 'ON DEMAND';
 	    vbatchprocess                  VARCHAR2 (150)  := NULL;
	    vmodulename                    varchar2 (150)  := 'VTR_JSON'; 
		vprocedurename                 VARCHAR2 (255)  := 'GEAR_JSON';
		vtablename                     VARCHAR2 (50)   := '';	    
		ilogid                         INT             := 0;
		VSQL                           varchar2 (4000);		
        errmsg           VARCHAR2 (2000);
        sql_stmt             VARCHAR2 (2000);
        vowner           VARCHAR2 (50)   :='CFDRS';
        ifsoseq          NUMBER          :=0;
        v_compliance_start  date;
        v_compliance_end    date;
        v_name             VARCHAR2 (25);
        v_debug  NUMBER;
        v_loop  NUMBER;
        v_maxdate date;
        v_result_length NUMBER := 0;
        v_outputed_length NUMBER := 1;   -- Because first position of a string in PL/SQL is 1, not 0
        
        ITERATION_LENGTH CONSTANT NUMBER := 4000;
        
        GEAR_CODE CONSTANT VARCHAR2(4) := 'CODE';
        GEAR_NAME CONSTANT VARCHAR2(4) := 'NAME';
        MESH_MINIMUM CONSTANT VARCHAR2(12) := 'MESH_MINIMUM';
        MESH_MAXIMUM CONSTANT VARCHAR2(12) := 'MESH_MAXIMUM';
        QUANTITY_MINIMUM CONSTANT VARCHAR2(16) := 'QUANTITY_MINIMUM';
        QUANTITY_MAXIMUM CONSTANT VARCHAR2(16) := 'QUANITY_MAXIMUM';
        SIZE_MINIMUM CONSTANT VARCHAR2(12) := 'SIZE_MINIMUM';
        SIZE_MAXIMUM CONSTANT VARCHAR2(12) := 'SIZE_MAXIMUM';
        HAULS_MINIMUM CONSTANT VARCHAR2(13) := 'HAULS_MINIMUM';
        HAULS_MAXIMUM CONSTANT VARCHAR2(13) := 'HAULS_MAXIMUM';     
        SOAK_MINIMUM CONSTANT VARCHAR2(12) := 'SOAK_MINIMUM';
        SOAK_MAXIMUM CONSTANT VARCHAR2(12) := 'SOAK_MAXIMUM';
        TYPE_CALCULATE CONSTANT VARCHAR2(15) := 'TYPE_CALCULATE';
        TYPE_VALIDATE CONSTANT VARCHAR2(15) := 'TYPE_VALIDATE';
        
        gear_object  JSON_OBJECT_T := json_object_t();
        gear_array  JSON_ARRAY_T := json_array_t();
        empty_array  JSON_ARRAY_T := json_array_t();
  begin
    DBMS_OUTPUT.ENABLE(1000000);
    
    for g_data in (with gear_data as (select gearcode as CODE
            ,GEARNM AS NAME
            ,to_char(MESH_MINIMUM) as  MESH_MINIMUM      -- cast as string
            ,to_char(MESH_MAXIMUM) as  MESH_MAXIMUM
            ,to_char(QUANTITY_MINIMUM) as  QUANTITY_MINIMUM
            ,to_char(QUANITY_MAXIMUM) as  QUANITY_MAXIMUM
            ,to_char(SIZE_MINIMUM) as  SIZE_MINIMUM
            ,to_char(SIZE_MAXIMUM) as  SIZE_MAXIMUM
            ,to_char(HAULS_MINIMUM) as  HAULS_MINIMUM
            ,to_char(HAULS_MAXIMUM) as  HAULS_MAXIMUM
            ,to_char(SOAK_MINIMUM) as  SOAK_MINIMUM
            ,to_char(SOAK_MAXIMUM) as  SOAK_MAXIMUM
            ,TYPE_CALCULATE
            ,TYPE_VALIDATE
        from (SELECT gearcode
            ,GEARNM
            ,CASE WHEN MINMESH = 0 and MAXMESH = 0 THEN NULL ELSE MINMESH END MESH_MINIMUM
            ,CASE WHEN MINMESH = 0 and MAXMESH = 0 THEN NULL ELSE MAXMESH END MESH_MAXIMUM
            ,CASE WHEN MINQTY = 0 and MAXQTY = 0 THEN NULL ELSE MINQTY END QUANTITY_MINIMUM
            ,CASE WHEN MINQTY = 0 and MAXQTY = 0 THEN NULL ELSE MAXQTY END QUANITY_MAXIMUM
            ,CASE WHEN MINSIZE = 0 and MAXSIZE = 0 THEN NULL ELSE MINSIZE END SIZE_MINIMUM
            ,CASE WHEN MINSIZE = 0 and MAXSIZE = 0 THEN NULL ELSE MAXSIZE END SIZE_MAXIMUM
            ,CASE WHEN MINHAUL = 0 and MAXHAUL = 0 THEN NULL ELSE MINHAUL END HAULS_MINIMUM
            ,CASE WHEN MINHAUL = 0 and MAXHAUL = 0 THEN NULL ELSE MAXHAUL END HAULS_MAXIMUM
            ,CASE WHEN MINSOAK = 0 and MAXSOAK = 0 THEN NULL ELSE MINSOAK END SOAK_MINIMUM
            ,CASE WHEN MINSOAK = 0 and MAXSOAK = 0 THEN NULL ELSE MAXSOAK END SOAK_MAXIMUM
            ,CASE WHEN SOAKTYPE = 'AVGHAUL' THEN 'AVERAGE' ELSE 'TOTAL' END TYPE_CALCULATE
            ,CASE WHEN SOAK_MAY_EXCEED_TRIP = 1
            THEN null
            WHEN SOAK_MAY_EXCEED_TRIP = 0 AND SOAKTYPE = 'AVGHAUL'
            THEN 'TIME:HAULS'
            ELSE 'TIME'
            END TYPE_VALIDATE
            FROM vtr.vlgear
            UNION
            SELECT 'PTLL' as GEARCODE
            ,GEARNM
            ,CASE WHEN MINMESH = 0 and MAXMESH = 0 THEN NULL ELSE MINMESH END MESH_MINIMUM
            ,CASE WHEN MINMESH = 0 and MAXMESH = 0 THEN NULL ELSE MAXMESH END MESH_MAXIMUM
            -------
            ,NULL as QUANTITY_MINIMUM
            ,NULL as QUANITY_MAXIMUM
            ,NULL as SIZE_MINIMUM
            ,NULL as SIZE_MAXIMUM
            ,NULL as HAULS_MINIMUM
            ,NULL as HAULS_MAXIMUM
            ,NULL as SOAK_MINIMUM
            ,NULL as SOAK_MAXIMUM
            ,CASE WHEN SOAKTYPE = 'AVGHAUL' THEN 'AVERAGE' ELSE 'TOTAL' END TYPE_CALCULATE
            ,CASE WHEN SOAK_MAY_EXCEED_TRIP = 1
            THEN null
            WHEN SOAK_MAY_EXCEED_TRIP = 0 AND SOAKTYPE = 'AVGHAUL'
            THEN 'TIME:HAULS'
            ELSE 'TIME'
            END TYPE_VALIDATE
            FROM vtr.vlgear g2
            WHERE g2.gearcode = 'PTL'
            UNION
            SELECT 'PTCL' as GEARCODE
            ,GEARNM
            ,CASE WHEN MINMESH = 0 and MAXMESH = 0 THEN NULL ELSE MINMESH END MESH_MINIMUM
            ,CASE WHEN MINMESH = 0 and MAXMESH = 0 THEN NULL ELSE MAXMESH END MESH_MAXIMUM
            -------
            ,NULL as QUANTITY_MINIMUM
            ,NULL as QUANITY_MAXIMUM
            ,NULL as SIZE_MINIMUM
            ,NULL as SIZE_MAXIMUM
            ,NULL as HAULS_MINIMUM
            ,NULL as HAULS_MAXIMUM
            ,NULL as SOAK_MINIMUM
            ,NULL as SOAK_MAXIMUM
            ,CASE WHEN SOAKTYPE = 'AVGHAUL' THEN 'AVERAGE' ELSE 'TOTAL' END TYPE_CALCULATE
            ,CASE WHEN SOAK_MAY_EXCEED_TRIP = 1
            THEN null
            WHEN SOAK_MAY_EXCEED_TRIP = 0 AND SOAKTYPE = 'AVGHAUL'
            THEN 'TIME:HAULS'
            ELSE 'TIME'
            END TYPE_VALIDATE
            FROM vtr.vlgear g3
            WHERE g3.gearcode = 'PTC'))
    select g.CODE, g.NAME, g.MESH_MINIMUM,g.MESH_MAXIMUM,g.QUANTITY_MINIMUM,g.QUANITY_MAXIMUM,g.SIZE_MINIMUM,g.SIZE_MAXIMUM
           ,g.HAULS_MINIMUM,g.HAULS_MAXIMUM,g.SOAK_MINIMUM,g.SOAK_MAXIMUM,g.TYPE_CALCULATE,g.TYPE_VALIDATE
    from gear_data g) loop
    
        gear_object.put(GEAR_CODE, g_data.CODE);
        gear_object.put(GEAR_NAME, g_data.NAME);
        gear_object.put(MESH_MINIMUM, g_data.MESH_MINIMUM);
        gear_object.put(MESH_MAXIMUM, g_data.MESH_MAXIMUM);
        gear_object.put(QUANTITY_MINIMUM, g_data.QUANTITY_MINIMUM);
        gear_object.put(QUANTITY_MAXIMUM, g_data.QUANITY_MAXIMUM);
        gear_object.put(SIZE_MINIMUM, g_data.SIZE_MINIMUM);
        gear_object.put(SIZE_MAXIMUM, g_data.SIZE_MAXIMUM);
        gear_object.put(HAULS_MINIMUM, g_data.HAULS_MINIMUM);
        gear_object.put(HAULS_MAXIMUM, g_data.HAULS_MAXIMUM);
        gear_object.put(SOAK_MINIMUM, g_data.SOAK_MINIMUM);
        gear_object.put(SOAK_MAXIMUM, g_data.SOAK_MAXIMUM);
        gear_object.put(TYPE_CALCULATE, g_data.TYPE_CALCULATE);
        gear_object.put(TYPE_VALIDATE, g_data.TYPE_VALIDATE);
        
         IF ( isi_CurrentDebugLevel >= VERBOSE_DEBUG_LEVEL ) THEN
            DBMS_OUTPUT.PUT_LINE('Gear record:' || gear_object.to_clob());
         END IF;
         
        --Add object to array
        gear_array.append(gear_object);
        --START cleaning of JSON object. This is needed per key
        gear_object.remove(GEAR_CODE);
        gear_object.remove(GEAR_NAME);
        gear_object.remove(MESH_MINIMUM);
        gear_object.remove(MESH_MAXIMUM);
        gear_object.remove(QUANTITY_MINIMUM);
        gear_object.remove(QUANTITY_MAXIMUM);
        gear_object.remove(SIZE_MINIMUM);
        gear_object.remove(SIZE_MAXIMUM);
        gear_object.remove(HAULS_MINIMUM);
        gear_object.remove(HAULS_MAXIMUM);
        gear_object.remove(SOAK_MINIMUM);
        gear_object.remove(SOAK_MAXIMUM);
        gear_object.remove(TYPE_CALCULATE);
        gear_object.remove(TYPE_VALIDATE);
    end loop;
    
    v_result := gear_array.to_clob();
    v_result_length := length(v_result);
    
     IF ( isi_CurrentDebugLevel >= ERROR_DEBUG_LEVEL ) THEN
        DBMS_OUTPUT.PUT_LINE('Gear JSON:');
        WHILE v_result_length > 0
        LOOP
            DBMS_OUTPUT.PUT_LINE(substr(v_result, v_outputed_length, ITERATION_LENGTH));
            v_outputed_length := v_outputed_length + ITERATION_LENGTH;
            v_result_length := v_result_length - ITERATION_LENGTH;
        END LOOP;
     END IF;
    
    --fso_admin.log_event (vbatchprocess, vmodulename, vprocedurename, ifsoseq, 'SUCCESSFUL', 'Successfully finished procedure.' ,vtablename,NULL,NULL,NULL, ilogid);
  EXCEPTION
    WHEN OTHERS THEN
        errmsg := errmsg || ' SQL Error on ' || vtablename ||' : ' || SQLERRM;
        
        v_result := empty_array.to_clob();
      --  set_run_status(ifsoseq, 'ABORT', -1, errmsg);
        --fso_admin.log_event ( vbatchprocess, vmodulename, vprocedurename, ifsoseq,'FAILED', 'Finished abnormally - '||errmsg,NULL,NULL,NULL,NULL, ilogid );
        DBMS_OUTPUT.PUT_LINE(VPROCEDURENAME || 'finished abnormally'||ERRMSG);
  end;
/****************************************************************************************************/
procedure dealer_json (v_input IN CLOB, v_result OUT clob )
  is
        
        vcronjob                       VARCHAR2(150)   := 'ON DEMAND';
 	    vbatchprocess                  VARCHAR2 (150)  := NULL;
	    vmodulename                    varchar2 (150)  := 'VTR_JSON'; 
		vprocedurename                 VARCHAR2 (255)  := 'DEALER_JSON';
		vtablename                     VARCHAR2 (50)   := '';	    
		ilogid                         INT             := 0;
		VSQL                           varchar2 (4000);		
        errmsg           VARCHAR2 (2000);
        sql_stmt             VARCHAR2 (2000);
        vowner           VARCHAR2 (50)   :='CFDRS';
        ifsoseq          NUMBER          :=0;
        v_compliance_start  date;
        v_compliance_end    date;
        v_name             VARCHAR2 (25);
        v_debug  NUMBER;
        v_loop  NUMBER := 0;
        v_maxdate date;
        v_result_length NUMBER := 0;
        v_outputed_length NUMBER := 1;   -- Because first position of a string in PL/SQL is 1, not 0
        v_timestamp NUMBER := 0;
        v_filecode NUMBER := 0;
        v_char_input VARCHAR2(100) := '';
        
        ITERATION_LENGTH CONSTANT NUMBER := 4000;
        
        DEALER_PERMIT CONSTANT VARCHAR2(20) := 'DEALER_PERMIT_NUMBER';
        DEALER_NAME CONSTANT VARCHAR2(11) := 'DEALER_NAME';
      
        dealer_object  JSON_OBJECT_T := json_object_t();
        dealer_array  JSON_ARRAY_T := json_array_t();
        empty_array  JSON_ARRAY_T := json_array_t();
  begin
    DBMS_OUTPUT.ENABLE(1000000);
    v_char_input := substr(NVL(v_input,'{}'),1,100);
    --v_char_input := substr(v_input,1,100);
    validate_input (v_char_input , v_filecode , v_timestamp);
    
    IF (v_filecode = 0) THEN
        v_result := '{}';  -- Return empty JSON if something goes wrong
        return;
    ELSIF (v_filecode = 1) THEN
        for d_data in (with dealer_data as (SELECT  DEALER_PERMIT_NUMBER,  upper(DEALER_NAME) DEALER_NAME
            FROM
            (
            SELECT dnum DEALER_PERMIT_NUMBER, dlr DEALER_NAME
                FROM
                (
                SELECT d.dnum, d.dlr, d.CITY, d.ST, row_number()
                OVER (PARTITION BY d.dnum ORDER BY d.year desc) r
                FROM PERMIT.DEALER d
                WHERE d.year >= (EXTRACT(YEAR from SYSDATE) - 5)
                AND d.dlr is not null
                )
                WHERE r = 1
                UNION
                SELECT 1 as dealer_permit_number,'Seized for Violations' as DEALER_NAME FROM dual
                UNION
                SELECT 2 as dealer_permit_number,'Sold or Retained as Bait' as DEALER_NAME FROM dual
                UNION
                SELECT 4 as dealer_permit_number,'Retained for Future Sale' as DEALER_NAME FROM dual
                UNION
                SELECT 5 as dealer_permit_number,'Sold to Non-Federal Dealer' as DEALER_NAME FROM dual
                UNION
                SELECT 6 as dealer_permit_number,'Sub Legal Catch Landed for Research' as DEALER_NAME FROM dual
                UNION
                SELECT 7 as dealer_permit_number,'Legal Catch Landed for Research (EFP Trips Only)' as DEALER_NAME FROM dual
                UNION
                SELECT 8 as dealer_permit_number,'Landed Unmarketable Catch (LUMF)' as DEALER_NAME FROM dual
                UNION
                SELECT 99998 as dealer_permit_number,'Home Consumption' as DEALER_NAME FROM dual)  )
        select d.dealer_permit_number, d.DEALER_NAME
        from dealer_data d) loop
        
            dealer_object.put(DEALER_PERMIT, d_data.DEALER_PERMIT_NUMBER);
            dealer_object.put(DEALER_NAME, d_data.DEALER_NAME);
            /*
            v_loop := v_loop + 1;
            if v_loop > 500 then
             exit;
            end if;
            */
             IF ( isi_CurrentDebugLevel >= VERBOSE_DEBUG_LEVEL ) THEN
                DBMS_OUTPUT.PUT_LINE('Dealer record:' || dealer_object.to_clob());
             END IF;
             
            --Add object to array
            dealer_array.append(dealer_object);
            --START cleaning of JSON object. This is needed per key
            dealer_object.remove(DEALER_PERMIT);
            dealer_object.remove(DEALER_NAME);
        end loop;
    END IF;
    
    v_result := dealer_array.to_clob();
    v_result_length := length(v_result);
    
     IF ( isi_CurrentDebugLevel >= ERROR_DEBUG_LEVEL ) THEN
        DBMS_OUTPUT.PUT_LINE('Dealer JSON:');
        WHILE v_result_length > 0
        LOOP
            DBMS_OUTPUT.PUT_LINE(substr(v_result, v_outputed_length, ITERATION_LENGTH));
            v_outputed_length := v_outputed_length + ITERATION_LENGTH;
            v_result_length := v_result_length - ITERATION_LENGTH;
        END LOOP;
     END IF;
    
    --fso_admin.log_event (vbatchprocess, vmodulename, vprocedurename, ifsoseq, 'SUCCESSFUL', 'Successfully finished procedure.' ,vtablename,NULL,NULL,NULL, ilogid);
  EXCEPTION
    WHEN OTHERS THEN
        errmsg := errmsg || ' SQL Error on ' || vtablename ||' : ' || SQLERRM;
        
        v_result := empty_array.to_clob();
      --  set_run_status(ifsoseq, 'ABORT', -1, errmsg);
        --fso_admin.log_event ( vbatchprocess, vmodulename, vprocedurename, ifsoseq,'FAILED', 'Finished abnormally - '||errmsg,NULL,NULL,NULL,NULL, ilogid );
        DBMS_OUTPUT.PUT_LINE(VPROCEDURENAME || 'finished abnormally'||ERRMSG);
  end;
/****************************************************************************************************/
 procedure dealer_jira_json (v_result OUT clob )
  is
        
        vcronjob                       VARCHAR2(150)   := 'ON DEMAND';
 	    vbatchprocess                  VARCHAR2 (150)  := NULL;
	    vmodulename                    varchar2 (150)  := 'VTR_JSON'; 
		vprocedurename                 VARCHAR2 (255)  := 'DEALER_JIRA_JSON';
		vtablename                     VARCHAR2 (50)   := '';	    
		ilogid                         INT             := 0;
		VSQL                           varchar2 (4000);		
        errmsg           VARCHAR2 (2000);
        sql_stmt             VARCHAR2 (2000);
        vowner           VARCHAR2 (50)   :='CFDRS';
        ifsoseq          NUMBER          :=0;
        v_compliance_start  date;
        v_compliance_end    date;
        v_name             VARCHAR2 (25);
        v_debug  NUMBER;
        v_loop  NUMBER := 0;
        v_maxdate date;
        v_result_length NUMBER := 0;
        v_outputed_length NUMBER := 1;   -- Because first position of a string in PL/SQL is 1, not 0
        
        ITERATION_LENGTH CONSTANT NUMBER := 4000;
        
        DEALER_PERMIT CONSTANT VARCHAR2(20) := 'DEALER_PERMIT_NUMBER';
        DEALER_NAME CONSTANT VARCHAR2(11) := 'DEALER_NAME';
        DEALER_PERMIT_WITH_NAME CONSTANT VARCHAR2(23) := 'DEALER_PERMIT_WITH_NAME';
      
        dealer_object  JSON_OBJECT_T := json_object_t();
        dealer_array  JSON_ARRAY_T := json_array_t();
        empty_array  JSON_ARRAY_T := json_array_t();
  begin
    DBMS_OUTPUT.ENABLE(1000000);
    
    for d_data in (with dealer_data as (SELECT  DEALER_PERMIT_NUMBER,  
                                                upper(DEALER_NAME) DEALER_NAME,
                                                upper(DEALER_PERMIT_WITH_NAME) DEALER_PERMIT_WITH_NAME
        FROM
        (
        SELECT dealer_permit_number
                ,dealer_name
                ,dealer_permit_with_name
                FROM FSO_ADMIN.jira_fh2_dealer_list
                ORDER BY dealer_permit_number ASC)  )
    select d.dealer_permit_number, d.DEALER_NAME, d.DEALER_PERMIT_WITH_NAME
    from dealer_data d) loop
    
        dealer_object.put(DEALER_PERMIT, d_data.DEALER_PERMIT_NUMBER);
        dealer_object.put(DEALER_NAME, d_data.DEALER_NAME);
        dealer_object.put(DEALER_PERMIT_WITH_NAME, d_data.DEALER_PERMIT_WITH_NAME);

         IF ( isi_CurrentDebugLevel >= VERBOSE_DEBUG_LEVEL ) THEN
            DBMS_OUTPUT.PUT_LINE('Dealer JIRA record:' || dealer_object.to_clob());
         END IF;
         
        --Add object to array
        dealer_array.append(dealer_object);
        --START cleaning of JSON object. This is needed per key
        dealer_object.remove(DEALER_PERMIT);
        dealer_object.remove(DEALER_NAME);
        dealer_object.remove(DEALER_PERMIT_WITH_NAME);
    end loop;
    
    v_result := dealer_array.to_clob();
    v_result_length := length(v_result);
    
     IF ( isi_CurrentDebugLevel >= ERROR_DEBUG_LEVEL ) THEN
        DBMS_OUTPUT.PUT_LINE('Dealer JIRA JSON:');
        WHILE v_result_length > 0
        LOOP
            DBMS_OUTPUT.PUT_LINE(substr(v_result, v_outputed_length, ITERATION_LENGTH));
            v_outputed_length := v_outputed_length + ITERATION_LENGTH;
            v_result_length := v_result_length - ITERATION_LENGTH;
        END LOOP;
     END IF;
    
    --fso_admin.log_event (vbatchprocess, vmodulename, vprocedurename, ifsoseq, 'SUCCESSFUL', 'Successfully finished procedure.' ,vtablename,NULL,NULL,NULL, ilogid);
  EXCEPTION
    WHEN OTHERS THEN
        errmsg := errmsg || ' SQL Error on ' || vtablename ||' : ' || SQLERRM;
        
        v_result := empty_array.to_clob();
      --  set_run_status(ifsoseq, 'ABORT', -1, errmsg);
        --fso_admin.log_event ( vbatchprocess, vmodulename, vprocedurename, ifsoseq,'FAILED', 'Finished abnormally - '||errmsg,NULL,NULL,NULL,NULL, ilogid );
        DBMS_OUTPUT.PUT_LINE(VPROCEDURENAME || 'finished abnormally'||ERRMSG);
  end;
/****************************************************************************************************/
 procedure vessel_jira_json (v_result OUT clob )
  is
        
        vcronjob                       VARCHAR2(150)   := 'ON DEMAND';
 	    vbatchprocess                  VARCHAR2 (150)  := NULL;
	    vmodulename                    varchar2 (150)  := 'VTR_JSON'; 
		vprocedurename                 VARCHAR2 (255)  := 'VESSEL_JIRA_JSON';
		vtablename                     VARCHAR2 (50)   := '';	    
		ilogid                         INT             := 0;
		VSQL                           varchar2 (4000);		
        errmsg           VARCHAR2 (2000);
        sql_stmt             VARCHAR2 (2000);
        vowner           VARCHAR2 (50)   :='CFDRS';
        ifsoseq          NUMBER          :=0;
        v_compliance_start  date;
        v_compliance_end    date;
        v_name             VARCHAR2 (25);
        v_debug  NUMBER;
        v_loop  NUMBER := 0;
        v_maxdate date;
        v_result_length NUMBER := 0;
        v_outputed_length NUMBER := 1;   -- Because first position of a string in PL/SQL is 1, not 0
        
        ITERATION_LENGTH CONSTANT NUMBER := 4000;
        
        VESSEL_PERMIT CONSTANT VARCHAR2(20) := 'VESSEL_PERMIT_NUMBER';
        VESSEL_NAME CONSTANT VARCHAR2(11) := 'VESSEL_NAME';
        VESSEL_PERMIT_WITH_NAME CONSTANT VARCHAR2(23) := 'VESSEL_PERMIT_WITH_NAME';
      
        vessel_object  JSON_OBJECT_T := json_object_t();
        vessel_array  JSON_ARRAY_T := json_array_t();
        empty_array  JSON_ARRAY_T := json_array_t();
  begin
    DBMS_OUTPUT.ENABLE(1000000);
    
    for v_data in (with vessel_data as (SELECT  VESSEL_PERMIT_NUMBER,  
                                                upper(VESSEL_NAME) VESSEL_NAME,
                                                upper(VESSEL_PERMIT_WITH_NAME) VESSEL_PERMIT_WITH_NAME
        FROM
        (
        SELECT vessel_permit_number
                ,vessel_name
                ,vessel_permit_with_name
                FROM FSO_ADMIN.jira_fh2_vessel_list
                ORDER BY vessel_permit_number ASC)  )
    select v.vessel_permit_number, v.VESSEL_NAME, v.VESSEL_PERMIT_WITH_NAME
    from vessel_data v) loop
    
        vessel_object.put(VESSEL_PERMIT, v_data.VESSEL_PERMIT_NUMBER);
        vessel_object.put(VESSEL_NAME, v_data.VESSEL_NAME);
        vessel_object.put(VESSEL_PERMIT_WITH_NAME, v_data.VESSEL_PERMIT_WITH_NAME);

         IF ( isi_CurrentDebugLevel >= VERBOSE_DEBUG_LEVEL ) THEN
            DBMS_OUTPUT.PUT_LINE('Vessel JIRA record:' || vessel_object.to_clob());
         END IF;
         
        --Add object to array
        vessel_array.append(vessel_object);
        --START cleaning of JSON object. This is needed per key
        vessel_object.remove(VESSEL_PERMIT);
        vessel_object.remove(VESSEL_NAME);
        vessel_object.remove(VESSEL_PERMIT_WITH_NAME);
    end loop;
    
    v_result := vessel_array.to_clob();
    v_result_length := length(v_result);
    
     IF ( isi_CurrentDebugLevel >= ERROR_DEBUG_LEVEL ) THEN
        DBMS_OUTPUT.PUT_LINE('Vessel JIRA JSON:');
        WHILE v_result_length > 0
        LOOP
            DBMS_OUTPUT.PUT_LINE(substr(v_result, v_outputed_length, ITERATION_LENGTH));
            v_outputed_length := v_outputed_length + ITERATION_LENGTH;
            v_result_length := v_result_length - ITERATION_LENGTH;
        END LOOP;
     END IF;
    
    --fso_admin.log_event (vbatchprocess, vmodulename, vprocedurename, ifsoseq, 'SUCCESSFUL', 'Successfully finished procedure.' ,vtablename,NULL,NULL,NULL, ilogid);
  EXCEPTION
    WHEN OTHERS THEN
        errmsg := errmsg || ' SQL Error on ' || vtablename ||' : ' || SQLERRM;
        
        v_result := empty_array.to_clob();
      --  set_run_status(ifsoseq, 'ABORT', -1, errmsg);
        --fso_admin.log_event ( vbatchprocess, vmodulename, vprocedurename, ifsoseq,'FAILED', 'Finished abnormally - '||errmsg,NULL,NULL,NULL,NULL, ilogid );
        DBMS_OUTPUT.PUT_LINE(VPROCEDURENAME || 'finished abnormally'||ERRMSG);
  end;
/****************************************************************************************************/
 procedure operator_json (v_input IN CLOB, v_result OUT clob )
  is
        
        vcronjob                       VARCHAR2(150)   := 'ON DEMAND';
 	    vbatchprocess                  VARCHAR2 (150)  := NULL;
	    vmodulename                    varchar2 (150)  := 'VTR_JSON'; 
		vprocedurename                 VARCHAR2 (255)  := 'OPERATOR_JSON';
		vtablename                     VARCHAR2 (50)   := '';	    
		ilogid                         INT             := 0;
		VSQL                           varchar2 (4000);		
        errmsg           VARCHAR2 (2000);
        sql_stmt             VARCHAR2 (2000);
        vowner           VARCHAR2 (50)   :='CFDRS';
        ifsoseq          NUMBER          :=0;
        v_compliance_start  date;
        v_compliance_end    date;
        v_name             VARCHAR2 (25);
        v_debug  NUMBER;
        v_loop  NUMBER := 0;
        v_maxdate date;
        v_result_length NUMBER := 0;
        v_outputed_length NUMBER := 1;   -- Because first position of a string in PL/SQL is 1, not 0
        
        ITERATION_LENGTH CONSTANT NUMBER := 4000;
        
        OPERATOR_KEY CONSTANT VARCHAR2(12) := 'OPERATOR_KEY';
        NAME_FIRST CONSTANT VARCHAR2(10) := 'NAME_FIRST';
        NAME_MIDDLE CONSTANT VARCHAR2(11) := 'NAME_MIDDLE';
        NAME_LAST CONSTANT VARCHAR2(9) := 'NAME_LAST';
      
        operator_object  JSON_OBJECT_T := json_object_t();
        operator_array  JSON_ARRAY_T := json_array_t();
        empty_array  JSON_ARRAY_T := json_array_t();
  begin
    DBMS_OUTPUT.ENABLE(1000000);
    
    for o_data in (with operator_data as (SELECT  OPERATOR_KEY,  
                                                upper(NAME_FIRST) NAME_FIRST,
                                                upper(NAME_MIDDLE) NAME_MIDDLE,
                                                upper(NAME_LAST) NAME_LAST
        FROM
        (
        SELECT DISTINCT jo.operator_key
            ,jo.name_first
            ,jo.name_middle
            ,jo.name_last
            FROM jops.op_permit_date t1
            ,jops.operator jo
            WHERE t1.operator_key = jo.operator_key
            AND (t1.date_expired IS NULL OR t1.date_expired > sysdate - 365)
            AND (t1.date_cancelled IS NULL OR t1.date_cancelled > sysdate - 365)
            ORDER BY jo.operator_key ASC)  )
    select o.operator_key, o.name_first, o.name_middle, o.name_last
    from operator_data o) loop
    
        operator_object.put(OPERATOR_KEY, o_data.OPERATOR_KEY);
        operator_object.put(NAME_FIRST, o_data.NAME_FIRST);
        operator_object.put(NAME_MIDDLE, o_data.NAME_MIDDLE);
        operator_object.put(NAME_LAST, o_data.NAME_LAST);

         IF ( isi_CurrentDebugLevel >= VERBOSE_DEBUG_LEVEL ) THEN
            DBMS_OUTPUT.PUT_LINE('Operator record:' || operator_object.to_clob());
         END IF;
         
        --Add object to array
        operator_array.append(operator_object);
        --START cleaning of JSON object. This is needed per key
        operator_object.remove(OPERATOR_KEY);
        operator_object.remove(NAME_FIRST);
        operator_object.remove(NAME_MIDDLE);
        operator_object.remove(NAME_LAST);
    end loop;
    
    v_result := operator_array.to_clob();
    v_result_length := length(v_result);
    
     IF ( isi_CurrentDebugLevel >= ERROR_DEBUG_LEVEL ) THEN
        DBMS_OUTPUT.PUT_LINE('Operator JSON:');
        WHILE v_result_length > 0
        LOOP
            DBMS_OUTPUT.PUT_LINE(substr(v_result, v_outputed_length, ITERATION_LENGTH));
            v_outputed_length := v_outputed_length + ITERATION_LENGTH;
            v_result_length := v_result_length - ITERATION_LENGTH;
        END LOOP;
     END IF;
    
    --fso_admin.log_event (vbatchprocess, vmodulename, vprocedurename, ifsoseq, 'SUCCESSFUL', 'Successfully finished procedure.' ,vtablename,NULL,NULL,NULL, ilogid);
  EXCEPTION
    WHEN OTHERS THEN
        errmsg := errmsg || ' SQL Error on ' || vtablename ||' : ' || SQLERRM;
        
        v_result := empty_array.to_clob();
      --  set_run_status(ifsoseq, 'ABORT', -1, errmsg);
        --fso_admin.log_event ( vbatchprocess, vmodulename, vprocedurename, ifsoseq,'FAILED', 'Finished abnormally - '||errmsg,NULL,NULL,NULL,NULL, ilogid );
        DBMS_OUTPUT.PUT_LINE(VPROCEDURENAME || 'finished abnormally'||ERRMSG);
  end;
/****************************************************************************************************/
END PKG_JSON;