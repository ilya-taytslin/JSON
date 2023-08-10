create or replace PACKAGE BODY PKG_JSON AS

 procedure chart_area_json (v_result OUT clob )
  is
  vcronjob                           VARCHAR2(150)  := 'ON DEMAND';
 	    vbatchprocess                  VARCHAR2 (150)  := NULL;
	    vmodulename                    varchar2 (150)  := 'DEALER_COMPLY'; 
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
    for a_data in (with area_data as (select distinct nemarea as area_code
        FROM noaa.loc2areas
        WHERE NOT REGEXP_LIKE (nemarea, '[0][0-4][0-9]$|[0][5][0]$')
        ORDER BY nemarea ASC)
    select a.area_code
    from area_data a) loop
    
        area_object.put('AREA_CODE', a_data.area_code);
    
        area_array.append(area_object);
        area_object.remove('AREA_CODE');
    end loop;
    
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
  vcronjob                           VARCHAR2(150)  := 'ON DEMAND';
 	    vbatchprocess                  VARCHAR2 (150)  := NULL;
	    vmodulename                    varchar2 (150)  := 'DEALER_COMPLY'; 
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
        
        gear_object  JSON_OBJECT_T := json_object_t();
        gear_array  JSON_ARRAY_T := json_array_t();
        empty_array  JSON_ARRAY_T := json_array_t();
  begin
    --select GET_FSO_SEQ_FNC into ifsoseq from dual; 
	--fso_admin.log_event (vbatchprocess, vmodulename, vprocedurename, ifsoseq, NULL, vprocedurename ||' -- currently executing',NULL,NULL,NULL,NULL, ilogid);

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
    
        gear_object := json_object_t();
        gear_object.put('CODE', g_data.CODE);
        gear_object.put('NAME', g_data.NAME);
        gear_object.put('MESH_MINIMUM', g_data.MESH_MINIMUM);
        gear_object.put('MESH_MAXIMUM', g_data.MESH_MAXIMUM);
        gear_object.put('QUANTITY_MINIMUM', g_data.QUANTITY_MINIMUM);
        gear_object.put('QUANITY_MAXIMUM', g_data.QUANITY_MAXIMUM);
        gear_object.put('SIZE_MINIMUM', g_data.SIZE_MINIMUM);
        gear_object.put('SIZE_MAXIMUM', g_data.SIZE_MAXIMUM);
        gear_object.put('HAULS_MINIMUM', g_data.HAULS_MINIMUM);
        gear_object.put('HAULS_MAXIMUM', g_data.HAULS_MAXIMUM);
        gear_object.put('SOAK_MINIMUM', g_data.SOAK_MINIMUM);
        gear_object.put('SOAK_MAXIMUM', g_data.SOAK_MAXIMUM);
        gear_object.put('TYPE_CALCULATE', g_data.TYPE_CALCULATE);
        gear_object.put('TYPE_VALIDATE', g_data.TYPE_VALIDATE);
    
        gear_array.append(gear_object);
        gear_object.remove('CODE');   -- repeat for each field
        gear_object.remove('NAME');
        gear_object.remove('MESH_MINIMUM');
        gear_object.remove('MESH_MAXIMUM');
        gear_object.remove('QUANTITY_MINIMUM');
        gear_object.remove('QUANTITY_MAXIMUM');
        gear_object.remove('SIZE_MINIMUM');
        gear_object.remove('SIZE_MAXIMUM');
        gear_object.remove('HAULS_MINIMUM');
        gear_object.remove('HAULS_MAXIMUM');
        gear_object.remove('SOAK_MINIMUM');
        gear_object.remove('SOAK_MAXIMUM');
        gear_object.remove('TYPE_CALCULATE');
        gear_object.remove('TYPE_VALIDATE');
    end loop;
    
    v_result := gear_array.to_clob();
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