/*
 * See the file LICENSE for redistribution information.
 *
 *Copyright (c) 2011, 2019 Oracle and/or its affiliates. All rights reserved.
 *
 */

/*
 * create or overwrite location file
 */
declare
  p_path       varchar2(4000);  /* oracle directory name */
  p_filename   varchar2(4000);  /* dest file name */
  p_clob       CLOB;          /* location entries */
  p_ret_code   number := 0;   /* return code, 0 for success, */
                              /*  non-zero for failures    */
                              /* contains sqlcode for ORA- errors */

  p_ret_msg    varchar2(4000) := 'SUC';
                          /* contains sqlerrm for ORA-errors
                          /*  - custom error messages */
                          /* is null for suc */

  /*local variables */
  output_file  utl_file.file_type;
  file1        utl_file.file_type;
  buf           varchar2(32767);
  bytesLeft     INTEGER;
  pos           INTEGER;
  chunk         INTEGER;
  done          boolean := false;
begin
  /* Get input parameters */

  p_path := ?;
  p_filename := ?;
  p_clob := ?;
  /*
     uncomment block for pl/sql testing
     p_path := '&1';
     p_filename := '&2';
     p_clob := '&3';
     p_overwrite := '&4';
   */

  begin
    output_file := utl_file.fopen(p_path,p_filename, 'W', 32767);
    /* Read from clob and write to file */
    bytesLeft := dbms_lob.getLength(p_clob);
    pos :=1;
    while bytesLeft > 0 loop
      chunk :=  32767;
       if bytesLeft < chunk then
         chunk := bytesLeft;
       end if;
      dbms_lob.read(p_clob, chunk, pos, buf );
      utl_file.put(output_file, buf);
      utl_file.fflush(output_file);
      bytesLeft := bytesLeft - chunk;
      pos := pos + chunk;
    end loop;

    utl_file.fclose(output_file);

    exception
     when others then
        p_ret_code := sqlcode;
        p_ret_msg := substr(sqlerrm, 1, 4000);
    end;

  ? := p_ret_code;
  ? := p_ret_msg;
end;



