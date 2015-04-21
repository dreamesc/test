CREATE OR REPLACE PACKAGE BODY z_cs_commission_sql AS
--
-- $Revision: 1809 $:
-- $Author: escender.sary $:
-- $HeadURL: svn://svn.weigandt-consulting.com/konzum/retail/branches/retail-13.2-2d/db/rms/schema/packages/z_commission_sql_b.sql $:
-- $Id: z_commission_sql_b.sql 1809 2015-04-09 12:19:24Z escender.sary $:
--
-- Request #29733: [Commision sales]Automatic creation of 'virtual transfers' and custom transaction
  /*******************************************************************************************************
  MODIFICATION HISTORY (add new entries on top!)
  DD.MM.YY   Who          Which program unit              Version              What
  -----------------------------------------------------------------------------------------
  01.04.15   E. Sary                                      v1.1                 added NIL for wh6 and wh9
  25.03.15   E. Sary                                      v1.0                 new
  11.03.15   S. Fedoronchuk                               v1.0                 new
  *******************************************************************************************************/
  --------------------------------------------------------------------------------
  ---
  Cg_code_name      CONSTANT VARCHAR2 (30) := 'Z_CS_COMMISSION_SQL';
  Cg_dateTime       CONSTANT VARCHAR2 (20) := TO_CHAR (SYSDATE, 'DD.MM.YYYY HH24:MI:SS');
  ---
  Cg_cs_code_type   CONSTANT kcs.code_detail.code_type%TYPE := 'CMS_VIRTUAL_WH';
  Cg_cs_code_wh6    CONSTANT kcs.code_detail.code%TYPE := 'WH6';
  Cg_cs_code_wh9    CONSTANT kcs.code_detail.code%TYPE := 'WH9';
  ---
  Lg_code_detail_wh6         kcs.code_detail.add_attr_number%TYPE;
  Lg_code_detail_wh9         kcs.code_detail.add_attr_number%TYPE;
  Lg_tsf_context_type        rms.tsfhead.context_type%TYPE := 'COMM';
  ---
  Lg_vdate                   rms.period.vdate%TYPE := get_vdate;
  Lg_last_eom_date           rms.system_variables.last_eom_date%TYPE;
  Lg_user                    all_users.username%TYPE := USER;

  ---
  ------------------------------------------------------------------------------------
  -- Private Function Declaration: log_debug
  ------------------------------------------------------------------------------------
  PROCEDURE log_debug (z_severity IN NUMBER, z_prog_name IN VARCHAR2, z_error_desc IN VARCHAR2) IS
  BEGIN
    IF vg_Debug
    OR z_severity = 1 THEN
      d.ww ('Run ID#' || Cg_dateTime || ' ' || z_prog_name || '-(' || z_severity || ') Desc:' || z_error_desc);
      DBMS_OUTPUT.PUT_LINE ('Run ID#' || Cg_dateTime || ' ' || z_prog_name || '-(' || z_severity || ') Desc:' || z_error_desc);
    END IF;
  END;

  ------------------------------------------------------------------------------------
  -- Private Function Declaration: init
  ------------------------------------------------------------------------------------
  FUNCTION init (IO_error_message IN OUT VARCHAR2)
    RETURN BOOLEAN IS
    c_ProgramName   CONSTANT VARCHAR2 (60) := Cg_code_name || '.INIT';

    ---
    CURSOR cr_code_dtl (I_code IN kcs.code_detail.code%TYPE) IS
      SELECT add_attr_number
        FROM kcs.code_detail
       WHERE code = I_code
         AND code_type = Cg_cs_code_type;

    CURSOR cr_last_eom_date  IS
      SELECT last_eom_date FROM rms.system_variables;

  BEGIN
    ---
    log_debug (0, c_ProgramName, 'START');

    ---
    OPEN cr_code_dtl (Cg_cs_code_wh6);

    FETCH cr_code_dtl INTO Lg_code_detail_wh6;

    IF (cr_code_dtl%NOTFOUND) THEN
      IO_error_message := 'Cursor cr_code_dtl No data found CODE_TYPE:' || Cg_cs_code_type || ' CODE:' || Cg_cs_code_wh6;

      CLOSE cr_code_dtl;

      RAISE PROGRAM_ERROR;
    END IF;

    CLOSE cr_code_dtl;

    OPEN cr_code_dtl (Cg_cs_code_wh9);

    FETCH cr_code_dtl INTO Lg_code_detail_wh9;

    IF (cr_code_dtl%NOTFOUND) THEN
      IO_error_message := 'Cursor cr_code_dtl No data found CODE_TYPE:' || Cg_cs_code_type || ' CODE:' || Cg_cs_code_wh9;

      CLOSE cr_code_dtl;

      RAISE PROGRAM_ERROR;
    END IF;

    CLOSE cr_code_dtl;

    OPEN cr_last_eom_date;

    FETCH cr_last_eom_date INTO Lg_last_eom_date;

    IF (cr_last_eom_date%NOTFOUND) THEN
      IO_error_message := 'Cursor cr_last_eom_date No data found.';

      CLOSE cr_last_eom_date;

      RAISE PROGRAM_ERROR;
    END IF;

    CLOSE cr_last_eom_date;

    ---
    log_debug (0, c_ProgramName, 'FINISH');
    ---
    RETURN TRUE;
  ---
  EXCEPTION
    WHEN PROGRAM_ERROR THEN
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
    WHEN OTHERS THEN
      IO_error_message :=
        SQL_LIB.CREATE_MSG ('PACKAGE_ERROR',
                            SQLERRM,
                            c_ProgramName,
                            TO_CHAR (SQLCODE));
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
  END;

  ------------------------------------------------------------------------------------
  -- Private Function Declaration: get_if_tran_data
  ------------------------------------------------------------------------------------
  FUNCTION get_if_tran_data (IO_error_message IN OUT VARCHAR2)
    RETURN BOOLEAN IS
    c_ProgramName   CONSTANT VARCHAR2 (60) := Cg_code_name || '.GET_IF_TRAN_DATA';
  BEGIN
    ---
    log_debug (0, c_ProgramName, 'START');

    ---
    INSERT INTO rms.z_comm_tran_data_gtt (item,
                                          loc_type,
                                          location,
                                          iftd_rowid,
                                          comm_il_total_cost,
                                          comm_il_units,
                                          comm_il_sign_units,
                                          il_row_number)
      SELECT td.item,
             td.loc_type,
             td.location,
             td.ROWID iftd_rowid,
             ABS (SUM (td.total_cost * DECODE (cd.commission_sign,  '-', -1,  '+', 1,  0)) OVER (PARTITION BY td.location, td.loc_type, td.item))
               comm_il_total_cost,
             ABS (SUM (td.units * DECODE (cd.commission_sign,  '-', -1,  '+', 1,  0)) OVER (PARTITION BY td.location, td.loc_type, td.item)) comm_il_units,
             SIGN (SUM (td.units * DECODE (cd.commission_sign,  '-', -1,  '+', 1,  0)) OVER (PARTITION BY td.location, td.loc_type, td.item))
               comm_il_sign_units,
             ROW_NUMBER () OVER (PARTITION BY td.location, td.loc_type, td.item ORDER BY 1) il_row_number
        FROM rms.z_v_found_data ci,
             rms.item_master im,
             rms.if_tran_data td,
             kcs.tran_data_codes_cd cd
       WHERE im.item = ci.item
         AND td.item = im.item
         AND td.dept = im.dept
         AND td.class = im.class
         AND td.subclass = im.subclass
         AND ci.comm_item = 'Y'
         AND cd.commission_ind = 'Y'
         AND cd.code = td.tran_code
         AND DECODE(td.tran_code, 22, td.gl_ref_no, -1) = DECODE(cd.code, 22, cd.reason, -1)
         AND td.units <> 0;

    ---
    log_debug (0, c_ProgramName, SQL%ROWCOUNT || ' inserted into rms.z_comm_tran_data_gtt');
    ---
    log_debug (0, c_ProgramName, 'FINISH');
    ---
    RETURN TRUE;
  ---
  EXCEPTION
    WHEN PROGRAM_ERROR THEN
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
    WHEN OTHERS THEN
      IO_error_message :=
        SQL_LIB.CREATE_MSG ('PACKAGE_ERROR',
                            SQLERRM,
                            c_ProgramName,
                            TO_CHAR (SQLCODE));
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
  END;

  ------------------------------------------------------------------------------------
  -- Private Function Declaration: comm_tsf_cre
  ------------------------------------------------------------------------------------
  FUNCTION comm_tsf_cre (IO_error_message   IN OUT VARCHAR2,
                         I_location         IN     rms.z_comm_tran_data_gtt.location%TYPE,
                         I_loc_type         IN     rms.z_comm_tran_data_gtt.loc_type%TYPE,
                         I_from_loc_type    IN     rms.tsfhead.from_loc_type%TYPE,
                         I_from_loc         IN     rms.tsfhead.from_loc%TYPE,
                         I_to_loc_type      IN     rms.tsfhead.to_loc_type%TYPE,
                         I_to_loc           IN     rms.tsfhead.to_loc%TYPE,
                         I_il_units_sign    IN     rms.z_comm_tran_data_gtt.comm_il_sign_units%TYPE,
                         O_tsf_no              OUT rms.tsfhead.tsf_no%TYPE)
    RETURN BOOLEAN IS
    c_ProgramName   CONSTANT VARCHAR2 (60) := Cg_code_name || '.COMM_TSF_CRE';
    ---
    L_return_char            VARCHAR2 (10) := NULL;
    L_tsf_no                 rms.tsfhead.tsf_no%TYPE := NULL;
    I_nil_loc                rms.z_comm_tran_data_gtt.location%TYPE;
  ---
  BEGIN
    ---
    log_debug (0, c_ProgramName, 'START');
    ---
    RMS.NEXT_TRANSFER_NUMBER (tsf_no => L_tsf_no, return_code => L_return_char, error_message => IO_error_message);
    IF L_return_char <> 'TRUE'
    OR L_tsf_no IS NULL THEN
      IO_error_message := 'RMS.NEXT_TRANSFER_NUMBER:' || IO_error_message;
      RAISE PROGRAM_ERROR;
    END IF;



    INSERT INTO rms.tsfhead (tsf_no,
                             tsf_parent_no,
                             from_loc_type,
                             from_loc,
                             to_loc_type,
                             to_loc,
                             exp_dc_date,
                             dept,
                             inventory_type,
                             tsf_type,
                             status,
                             freight_code,
                             routing_code,
                             create_date,
                             create_id,
                             approval_date,
                             approval_id,
                             delivery_date,
                             close_date,
                             ext_ref_no,
                             repl_tsf_approve_ind,
                             comment_desc,
                             context_type,
                             context_value)
         VALUES (L_tsf_no,
                 NULL,                                                                                                                        --TSF_PARENT_NO
                 I_from_loc_type,
                 I_from_loc,
                 I_to_loc_type,
                 I_to_loc,
                 Lg_vdate,
                 NULL,                                                                                                                                 --DEPT
                 'A',                                                                                              --INVENTORY_TYPE --Available for inventory
                 'MR',                                                                                                        --TSF_TYPE --Manual Requisition
                 'A',                                                                                                                                --STATUS
                 'N',                                                                                                                 --FREIGHT_CODE --Normal
                 NULL,                                                                                                                         --ROUTING_CODE
                 Lg_vdate,                                                                                                                      --CREATE_DATE
                 Lg_user,                                                                                                                         --CREATE_ID
                 Lg_vdate,                                                                                                                    --APPROVAL_DATE
                 Lg_user,                                                                                                                       --APPROVAL_ID
                 Lg_vdate,                                                                                                                    --DELIVERY_DATE
                 NULL,                                                                                                                           --CLOSE_DATE
                 NULL,                                                                                                                           --EXT_REF_NO
                 'N',                                                                                                                  --REPL_TSF_APPROVE_IND
                 'Komission Virtual Transfer',
                 Lg_tsf_context_type,
                 NULL);                                                                                                               --or cg_cs_cont_val_out


    ---
    log_debug (0, c_ProgramName, SQL%ROWCOUNT || ' inserted into rms.tsfhead');

    ---

    INSERT INTO rms.tsfdetail (tsf_no,
                               tsf_seq_no,
                               item,
                               tsf_qty,
                               tsf_cost,
                               supp_pack_size,
                               publish_ind,
                               updated_by_rms_ind)
      SELECT L_tsf_no,
             ROWNUM,
             t.item,
             t.comm_il_units,
             t.comm_il_total_cost / t.comm_il_units,
             isc.supp_pack_size,
             'N',
             'Y'
        FROM rms.z_comm_tran_data_gtt t, rms.item_supp_country isc
       WHERE t.location = I_location
         AND t.loc_type = I_loc_type
         AND t.comm_il_sign_units = I_il_units_sign
         AND t.il_row_number = 1
         AND t.comm_il_units <> 0
         AND t.item = isc.item
         AND isc.primary_supp_ind = 'Y'
         AND isc.primary_country_ind = 'Y';

    ---
    log_debug (0, c_ProgramName, SQL%ROWCOUNT || ' inserted into rms.tsfdetail');
    ---
    IF I_from_loc = I_location THEN
      I_nil_loc := I_to_loc;
    ELSE
      I_nil_loc := I_from_loc;
    END IF; 
    -- item-ranging
    FOR rec IN (SELECT item
                  FROM tsfdetail td
                 WHERE tsf_no = L_tsf_no) LOOP
      IF NOT RMS.NEW_ITEM_LOC (IO_error_message,
                               rec.item,
                               I_nil_loc,
                               NULL,
                               NULL,
                               'W',
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               NULL,
                               FALSE) THEN
        IO_error_message :=
          'RMS.NEW_ITEM_LOC - Item:' || rec.item || ' Loc:' || I_nil_loc  || ' Error:' || IO_error_message;
        RAISE PROGRAM_ERROR;
      END IF;
    END LOOP;



    -- UPDATE RESERVE AND EXPECTED QTY-s according to TSF

    IF NOT RMS.TRANSFER_SQL.UPD_TSF_RESV_EXP (O_error_message     => IO_error_message,
                                              I_tsf_no            => L_tsf_no,
                                              I_add_delete_ind    => 'A',
                                              I_appr_second_leg   => FALSE) THEN
      IO_error_message := 'RMS.TRANSFER_SQL.UPD_TSF_RESV_EXP - Tsf_no:' || L_tsf_no || ' Error:' || IO_error_message;
      RAISE PROGRAM_ERROR;
    END IF;

    O_tsf_no := L_tsf_no;
    log_debug (0, c_ProgramName, 'FINISH');
    ---
    RETURN TRUE;
  ---
  EXCEPTION
    WHEN PROGRAM_ERROR THEN
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
    WHEN OTHERS THEN
      IO_error_message :=
        SQL_LIB.CREATE_MSG ('PACKAGE_ERROR',
                            SQLERRM,
                            c_ProgramName,
                            TO_CHAR (SQLCODE));
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
  END;

  ------------------------------------------------------------------------------------
  -- Private Function Declaration: receive_comm_tsf
  ------------------------------------------------------------------------------------
  FUNCTION receive_comm_tsf (IO_error_message IN OUT VARCHAR2, I_tsf_no IN tsfhead.tsf_no%TYPE)
    RETURN BOOLEAN IS
    c_ProgramName   CONSTANT VARCHAR2 (60) := Cg_code_name || '.RECEIVE_COMM_TSF';
    ---
    ------------------------------------
    L_shipment               rms.shipment.shipment%TYPE;
    L_bol_no                 rms.shipment.bol_no%TYPE;
    L_shipment_to_loc        rms.shipment.to_loc%TYPE;
    ---------------------
    L_item_recv_tbl          RMS.RECEIVE_SQL.ITEM_RECV_TABLE;
  BEGIN
    ---
    log_debug (0, c_ProgramName, 'START');

    --------------------------------------------------------
    --find shipment and bol_no
    SELECT MAX (s.shipment), MAX (s.bol_no), MAX (s.to_loc)
      INTO L_shipment, L_bol_no, L_shipment_to_loc
      FROM shipsku ss, shipment s
     WHERE ss.distro_no = I_tsf_no
       AND ss.distro_type = 'T'
       AND s.shipment = ss.shipment;

    log_debug (0, c_ProgramName, 'shipment = ' || L_shipment || ' bol_no = ' || L_bol_no);

    IF L_shipment IS NULL THEN
      IO_error_message := 'Unknown shipment';
      RAISE PROGRAM_ERROR;
    END IF;

    --------------------------------------------------------
    --fill object: copy and modify from FETCH_ITEM_BULK
    SELECT DISTINCT s.item,
                    im.item_desc,
                    s.carton,
                    s.distro_no,
                    s.distro_type,
                    s.qty_expected,
                    s.qty_expected AS qty_received,                                                                                         --should be equil
                    im.standard_uom,
                    s.weight_received,
                    s.weight_received_uom,
                    NULL,                                                                                                                       -- error code
                    'TRUE',                                                                                                                    -- return code
                    'ATS'                                                                                                     -- inv_status Available To Sell
      BULK COLLECT INTO L_item_recv_tbl
      FROM shipsku s, item_master im
     WHERE s.item = im.item
       AND s.status_code != 'R'
       AND s.shipment = L_shipment;

    log_debug (0, c_ProgramName, 'select rows ' || SQL%ROWCOUNT);
    -----------------------------------------------------
    --call RMS procedure for Receiving
    RMS.RECEIVE_SQL.RECEIVE_ITEMS (O_item_receiving_table   => L_item_recv_tbl,
                                   I_shipment               => L_shipment,
                                   I_distro_content         => 'T',
                                   I_location               => L_shipment_to_loc,
                                   I_asn_bol_no             => L_bol_no,
                                   I_order_no               => NULL,
                                   I_receipt_date           => Lg_vdate,
                                   I_disposition            => 'ATS'                                                                      --Available To Sell
                                                                    );

    IF L_item_recv_tbl (1).return_code = 'FALSE' THEN
      IO_error_message := 'RMS.RECEIVE_SQL.RECEIVE_ITEMS - ' || L_item_recv_tbl (1).error_message;
      RAISE PROGRAM_ERROR;
    END IF;

    ---
    log_debug (0, c_ProgramName, 'FINISH');
    ---
    RETURN TRUE;
  ---
  EXCEPTION
    WHEN PROGRAM_ERROR THEN
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
    WHEN OTHERS THEN
      IO_error_message :=
        SQL_LIB.CREATE_MSG ('PACKAGE_ERROR',
                            SQLERRM,
                            c_ProgramName,
                            TO_CHAR (SQLCODE));
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
  END;

  ------------------------------------------------------------------------------------
  -- Private Function Declaration: close_comm_tsf
  ------------------------------------------------------------------------------------
  FUNCTION close_comm_tsf (IO_error_message IN OUT VARCHAR2, I_tsf_no IN tsfhead.tsf_no%TYPE)
    RETURN BOOLEAN IS
    c_ProgramName   CONSTANT VARCHAR2 (60) := Cg_code_name || '.CLOSE_COMM_TSF';
    ---
  BEGIN
    ---
    log_debug (0, c_ProgramName, 'START');

    IF NOT RMS.TRANSFER_SQL.UPD_QTYS_WHEN_CLOSE (IO_error_message, I_tsf_no) THEN
      IO_error_message := 'RMS.TRANSFER_SQL.UPD_QTYS_WHEN_CLOSE - Tsf_no' || I_tsf_no || ' Error:' || IO_error_message;
      RAISE PROGRAM_ERROR;
    END IF;

    UPDATE tsfdetail
       SET cancelled_qty = tsf_qty - NVL (ship_qty, 0)
     WHERE tsf_no = I_tsf_no
       AND tsf_qty <> NVL (ship_qty, 0);

    UPDATE tsfhead
       SET status = 'C', close_date = Lg_vdate
     WHERE tsf_no = I_tsf_no;

    DELETE FROM doc_close_queue
          WHERE doc_type = 'T'
            AND doc = I_tsf_no;

    RETURN TRUE;
  ---
  EXCEPTION
    WHEN PROGRAM_ERROR THEN
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
    WHEN OTHERS THEN
      IO_error_message :=
        SQL_LIB.CREATE_MSG ('PACKAGE_ERROR',
                            SQLERRM,
                            c_ProgramName,
                            TO_CHAR (SQLCODE));
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
  END;

  ------------------------------------------------------------------------------------
  -- Private Function Declaration: write_tran_data
  ------------------------------------------------------------------------------------
  FUNCTION write_tran_data (IO_error_message       IN OUT VARCHAR2,
                            I_location             IN     rms.z_comm_tran_data_gtt.location%TYPE,
                            I_loc_type             IN     rms.z_comm_tran_data_gtt.loc_type%TYPE,
                            I_comm_il_sign_units   IN     rms.z_comm_tran_data_gtt.comm_il_sign_units%TYPE)
    RETURN BOOLEAN IS
    c_ProgramName   CONSTANT VARCHAR2 (60) := Cg_code_name || '.WRITE_TRAN_DATA';
    ---
    L_tran_code              rms.tran_data.tran_code%TYPE;
    L_unit_cost              rms.tran_data.total_cost%TYPE;
    L_total_cost             rms.tran_data.total_cost%TYPE;
    ---
    L_dept                   rms.item_master.dept%TYPE;
    L_class                  rms.item_master.class%TYPE;
    L_subclass               rms.item_master.subclass%TYPE;

    ---
    CURSOR cr_comm_il  IS
      SELECT t.item, t.comm_il_units
        FROM rms.z_comm_tran_data_gtt t
       WHERE t.location = I_location
         AND t.loc_type = I_loc_type
         AND t.comm_il_sign_units = I_comm_il_sign_units
         AND t.il_row_number = 1;

    CURSOR cr_item_info (I_item rms.item_master.item%TYPE) IS
      SELECT im.dept, im.class, im.subclass
        FROM rms.item_master im
       WHERE im.item = I_item;

    CURSOR cr_costpricewithfee (
      I_item     rms.item_master.item%TYPE) IS
      SELECT /*+ ordered index(fc PK_FUTURE_COST) index(its PK_ITEM_SUPPLIER) index(sups PK_SUPS) use_nl(fc its sups ied cr vi) push_pred(cr)*/
            fc .net_cost * cr.exchange_rate                                                                                                      -- net_cost,
             + NVL (ied.est_dty_value, 0)                                                                                                   -- est_dty_value,
             + NVL (ied.est_exc_value, 0)                                                                                                   -- est_exc_value,
             + NVL (ied.est_exp_value, 0)                                                                                                   -- est_exp_value,
             + NVL (ied.est_own_value, 0)                                                                                                   -- est_own_value,
             + NVL (ied.est_ots_value, 0)                                                                                                   -- est_ots_value,
             + NVL (ied.est_trans_c_value, 0)                                                                                           -- est_trans_c_value,
             + NVL (ied.est_pkg_value, 0)                                                                                                   -- est_pkg_value,
             + NVL (ied.est_gst_value, 0)                                                                                                   -- est_gst_value,
             + NVL (ied.est_otr_value, 0)                                                                                                   -- est_otr_value,
             + NVL (ied.est_ambalaza_c_value, 0)                                                                                     -- est_ambalaza_c_value,
             + NVL (ied.est_import_t_value, 0)                                                                                         -- est_import_t_value,
             + NVL (ied.est_fee1_value, 0)                                                                                                 -- est_fee1_value,
             + NVL (ied.est_fee2_value, 0)                                                                                                  -- est_fee2_value
        FROM rms.future_cost fc,
             (  SELECT /*+ index(mv z_mv_cur_conv_ra_to_cur_bmix) use_nl(mv so c ph)*/
                      mv.from_currency AS currency_code,
                       c.currency_cost_dec AS currency_cost_dec,                                                          -- ea: DFR page 66, Decision mfp#33
                       ph.item AS item,
                       MAX (mv.exchange_rate) KEEP (DENSE_RANK LAST ORDER BY mv.effective_date) AS exchange_rate
                  FROM rms.mv_currency_conversion_rates mv,
                       (  SELECT ph2.item AS item, MAX (ph2.action_date) AS curr_active_date -- ea: Date for valid exchange rate for cost (as part of Problem #26)
                            FROM price_hist ph2
                           WHERE ph2.tran_type IN (0, 2)
                             AND ph2.loc = 29999
                        GROUP BY ph2.item) ph,
                       rms.system_options so,
                       rms.currencies c
                 WHERE mv.to_currency = so.currency_code                                                -- ea: Konzum legacy is in Primary RMS currency (HRK)
                   AND mv.exchange_type = 'C'                                                                              -- ea: DFR page 625, Decision #340
                   AND mv.to_currency = c.currency_code                                                    -- ea: Digits after coma chosen to target currency
                   AND mv.effective_date <= ph.curr_active_date                                                                 -- ea: As part of Problem #26
              GROUP BY mv.from_currency, c.currency_cost_dec, ph.item) cr,                                           -- ea: cr is Required currency rate view
             (  SELECT /*+ index(ie MV_ITEM_EXP_DETAIL_I1) index(it.im PK_ITEM) index(it.pi PACKITEM_I3) index(it.lov PK_UDA_ITEM_LOV)
                       USE_NL(it.pi it.im it.lov) USE_NL(ie it)*/
                       it.item,
                       ie.supplier,
                       SUM (DECODE (ie.z_comp_id, 'NCDTY', ie.est_exp_value * it.pack_qty, 0)) AS est_dty_value,
                       SUM (DECODE (ie.z_comp_id,  'MCEXC', ie.est_exp_value * it.pack_qty,  'NCEXC', ie.est_exp_value * it.pack_qty,  0)) AS est_exc_value,
                       SUM (DECODE (ie.z_comp_id,  'MCEXP', ie.est_exp_value * it.pack_qty,  'NCEXP', ie.est_exp_value * it.pack_qty,  0)) AS est_exp_value,
                       SUM (DECODE (ie.z_comp_id,  'MCOWT', ie.est_exp_value * it.pack_qty,  'NCOWT', ie.est_exp_value * it.pack_qty,  0)) AS est_own_value,
                       SUM (DECODE (ie.z_comp_id,  'MCOTS', ie.est_exp_value * it.pack_qty,  'NCOTS', ie.est_exp_value * it.pack_qty,  0)) AS est_ots_value,
                       SUM (DECODE (ie.z_comp_id,  'MCTRANS_C', ie.est_exp_value * it.pack_qty,  'NCTRANS_C', ie.est_exp_value * it.pack_qty,  0))
                         AS est_trans_c_value,
                       SUM (DECODE (ie.z_comp_id,  'MCPKG', ie.est_exp_value * it.pack_qty,  'NCPKG', ie.est_exp_value * it.pack_qty,  0)) AS est_pkg_value,
                       SUM (DECODE (ie.z_comp_id,  'MCGST', ie.est_exp_value * it.pack_qty,  'NCGST', ie.est_exp_value * it.pack_qty,  0)) AS est_gst_value,
                       SUM (DECODE (ie.z_comp_id,  'MCOTR', ie.est_exp_value * it.pack_qty,  'NCOTR', ie.est_exp_value * it.pack_qty,  0)) AS est_otr_value,
                       SUM (DECODE (ie.z_comp_id,  'MCAMBALAZA_C', ie.est_exp_value * it.pack_qty,  'NCAMBALAZA_C', ie.est_exp_value * it.pack_qty,  0))
                         AS est_ambalaza_c_value,
                       SUM (DECODE (ie.z_comp_id,  'MCIMPORT_T', ie.est_exp_value * it.pack_qty,  'NCIMPORT_T', ie.est_exp_value * it.pack_qty,  0))
                         AS est_import_t_value,
                       SUM (DECODE (ie.z_comp_id,  'MCFEE1', ie.est_exp_value * it.pack_qty,  'NCFEE1', ie.est_exp_value * it.pack_qty,  0)) AS est_fee1_value,
                       SUM (DECODE (ie.z_comp_id,  'MCFEE2', ie.est_exp_value * it.pack_qty,  'NCFEE2', ie.est_exp_value * it.pack_qty,  0)) AS est_fee2_value
                  FROM (SELECT DISTINCT im.item AS item,
                                        DECODE (im.pack_type, 'B', pi.pack_qty, 1) AS pack_qty,
                                        DECODE (im.pack_type, 'B', pi.item, im.item) AS exp_item,
                                        DECODE (im.pack_type, 'B', DECODE (lov.uda_value,  2, 'Y',  3, 'Y',  'N'), 'Y') AS flag -- ea: CB Packs may not have Fee3 on Top
                          FROM rms.item_master im, rms.packitem pi, rms.uda_item_lov lov
                         WHERE im.item = pi.pack_no(+)
                           AND im.item = lov.item
                           AND lov.uda_id = 10) it,                                                                        -- ea: Cost components and/or Fees
                       rms.mv_item_exp_detail ie
                 WHERE it.exp_item = ie.item
              GROUP BY it.item, ie.supplier) ied,
             rms.sups,
             rms.item_supplier its,                                      -- ea: Excluding non-Primary Suppliers, if several sups will be created for an item.
             (  SELECT /*+ index(vi2 VAT_ITEM_I1)*/
                      vi2.item, MAX (vi2.vat_rate) KEEP (DENSE_RANK LAST ORDER BY vi2.active_date) AS vat_rate
                  FROM rms.vat_item vi2
                 WHERE vi2.vat_region = 1000                                                           -- ea: All konzum selling locations are liable for VAT
                   AND vi2.active_date <= Lg_vdate                                                                                     -- ea: Actual VAT rate
              GROUP BY vi2.item) vi
       WHERE its.primary_supp_ind = 'Y'
         AND fc.primary_supp_country_ind = 'Y'
         AND fc.supplier = its.supplier
         AND fc.currency_code = cr.currency_code
         AND fc.item = cr.item                                                                                        -- ea: as part of Problem #26 resolving
         AND fc.item = its.item
         AND fc.item = vi.item
         AND fc.location = 29999
         AND fc.supplier = sups.supplier
         AND its.item = ied.item(+)                                                                             -- ea: items may not have components attached
         AND its.supplier = ied.supplier(+)                                                                                                                --
         AND fc.active_date = (SELECT /*+ index(fc1 PK_FUTURE_COST)*/
                                     MAX (fc1.active_date)
                                 FROM rms.future_cost fc1
                                WHERE fc1.active_date <= Lg_vdate                                                                   -- ea: actual cost values
                                  AND fc1.item = fc.item
                                  AND fc1.supplier = fc.supplier
                                  AND fc1.origin_country_id = fc.origin_country_id
                                  AND fc1.location = fc.location)
         AND its.item = I_item;

  ---
  BEGIN
    ---
    log_debug (0, c_ProgramName, 'START');

    ---

    FOR cr_comm_il_row IN cr_comm_il LOOP
      OPEN cr_item_info (cr_comm_il_row.item);

      FETCH cr_item_info
        INTO L_dept, L_class, L_subclass;

      IF cr_item_info%NOTFOUND THEN
        IO_error_message := 'Cursor cr_item_info No data found ITEM:' || cr_comm_il_row.item;

        CLOSE cr_item_info;

        RAISE PROGRAM_ERROR;
      END IF;

      CLOSE cr_item_info;

      OPEN cr_costpricewithfee (cr_comm_il_row.item);

      FETCH cr_costpricewithfee INTO L_unit_cost;

      IF cr_costpricewithfee%NOTFOUND THEN
        L_unit_cost := 0;
      END IF;

      CLOSE cr_costpricewithfee;

      L_total_cost := L_unit_cost * cr_comm_il_row.comm_il_units * I_comm_il_sign_units;
      L_tran_code := 95;
      IF rms.stkledgr_sql.tran_data_insert (O_error_message         => IO_error_message,
                                            I_item                  => cr_comm_il_row.item,
                                            I_dept                  => L_dept,
                                            I_class                 => L_class,
                                            I_subclass              => L_subclass,
                                            I_location              => I_location,
                                            I_loc_type              => I_loc_type,
                                            I_tran_date             => Lg_vdate,
                                            IO_tran_code            => L_tran_code,
                                            I_adj_code              => NULL,
                                            I_units                 => cr_comm_il_row.comm_il_units * I_comm_il_sign_units,
                                            IO_total_cost           => L_total_cost,
                                            I_total_retail          => NULL,
                                            I_ref_no_1              => NULL,
                                            I_ref_no_2              => NULL,
                                            I_tsf_source_location   => NULL,
                                            I_tsf_source_loc_type   => NULL,
                                            I_old_unit_retail       => NULL,
                                            I_new_unit_retail       => NULL,
                                            I_source_dept           => NULL,
                                            I_source_class          => NULL,
                                            I_source_subclass       => NULL,
                                            I_pgm_name              => Cg_code_name,
                                            I_gl_ref_no             => 1) = FALSE THEN
        IO_error_message := 'RMS.STKLEDGR_SQL.TRAN_DATA_INSERT - ' || IO_error_message;
        RAISE PROGRAM_ERROR;
      END IF;
    END LOOP;

    ---
    log_debug (0, c_ProgramName, 'FINISH');
    ---
    RETURN TRUE;
  ---
  EXCEPTION
    WHEN PROGRAM_ERROR THEN
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
    WHEN OTHERS THEN
      IO_error_message :=
        SQL_LIB.CREATE_MSG ('PACKAGE_ERROR',
                            SQLERRM,
                            c_ProgramName,
                            TO_CHAR (SQLCODE));
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
  END;

  ------------------------------------------------------------------------------------
  -- Private Function Declaration: proc_cust_data
  ------------------------------------------------------------------------------------
  FUNCTION proc_cust_data (IO_error_message       IN OUT VARCHAR2,
                           I_location             IN     rms.z_comm_tran_data_gtt.location%TYPE,
                           I_loc_type             IN     rms.z_comm_tran_data_gtt.loc_type%TYPE,
                           I_comm_il_sign_units   IN     rms.z_comm_tran_data_gtt.comm_il_sign_units%TYPE,
                           I_tsf_in_no            IN     tsfhead.tsf_no%TYPE,
                           I_tsf_out_no           IN     tsfhead.tsf_no%TYPE)
    RETURN BOOLEAN IS
    c_ProgramName   CONSTANT VARCHAR2 (60) := Cg_code_name || '.PROC_CUST_DATA';
    ---
    ------------------------------------
    L_iftd_rowid             ROWID;

    ---------------------
    TYPE cd_iftd_rowid_rec IS RECORD
    (
      tsf_in_30    ROWID,
      tsf_in_32    ROWID,
      tsf_out_30   ROWID,
      tsf_out_32   ROWID,
      td_code_95   ROWID
    );

    L_cd_iftd_rowid          cd_iftd_rowid_rec := NULL;
    L_supplier_id            rms.item_supplier.supplier%TYPE;
    L_tsf_to_loc             rms.tsfhead.tsf_no%TYPE;

    ---------------------

    CURSOR cr_tran_data  IS
      SELECT *
        FROM tran_data
       WHERE (ref_no_1 IN (I_tsf_in_no, I_tsf_out_no)
           OR tran_code = 95
          AND gl_ref_no = 1)
      FOR UPDATE;

    CURSOR cr_item_supp (I_item rms.item_supplier.item%TYPE) IS
      SELECT supplier
        FROM rms.item_supplier
       WHERE item = I_item
         AND primary_supp_ind = 'Y';

    CURSOR cr_tsf_to_loc (I_tsf_no rms.tsfhead.tsf_no%TYPE) IS
      SELECT to_loc
        FROM rms.tsfhead
       WHERE tsf_no = I_tsf_no;

    cr_tran_data_row         cr_tran_data%ROWTYPE;
  BEGIN
    ---
    log_debug (0, c_ProgramName, 'START');

    ---

    OPEN cr_tran_data;

    LOOP
      FETCH cr_tran_data INTO cr_tran_data_row;

      EXIT WHEN cr_tran_data%NOTFOUND;

      ---
      INSERT INTO if_tran_data (item,
                                dept,
                                class,
                                subclass,
                                pack_ind,
                                loc_type,
                                location,
                                tran_date,
                                tran_code,
                                adj_code,
                                units,
                                total_cost,
                                total_retail,
                                ref_no_1,
                                ref_no_2,
                                gl_ref_no,
                                old_unit_retail,
                                new_unit_retail,
                                pgm_name,
                                sales_type,
                                vat_rate,
                                av_cost,
                                ref_pack_no,
                                tran_data_timestamp)
           VALUES (cr_tran_data_row.item,
                   cr_tran_data_row.dept,
                   cr_tran_data_row.class,
                   cr_tran_data_row.subclass,
                   cr_tran_data_row.pack_ind,
                   cr_tran_data_row.loc_type,
                   cr_tran_data_row.location,
                   cr_tran_data_row.tran_date,
                   cr_tran_data_row.tran_code,
                   cr_tran_data_row.adj_code,
                   cr_tran_data_row.units,
                   cr_tran_data_row.total_cost,
                   cr_tran_data_row.total_retail,
                   cr_tran_data_row.ref_no_1,
                   cr_tran_data_row.ref_no_2,
                   cr_tran_data_row.gl_ref_no,
                   cr_tran_data_row.old_unit_retail,
                   cr_tran_data_row.new_unit_retail,
                   cr_tran_data_row.pgm_name,
                   cr_tran_data_row.sales_type,
                   cr_tran_data_row.vat_rate,
                   cr_tran_data_row.av_cost,
                   cr_tran_data_row.ref_pack_no,
                   cr_tran_data_row.timestamp)
        RETURNING ROWID
             INTO L_iftd_rowid;

      -- fill kcs.tran_data_history_cd
      IF cr_tran_data_row.tran_code IN (30, 32, 95) THEN
        -- get supplier number
        OPEN cr_item_supp (cr_tran_data_row.item);

        FETCH cr_item_supp INTO L_supplier_id;

        IF cr_item_supp%NOTFOUND THEN
          IO_error_message := 'Cursor cr_item_supp No data found Item:' || cr_tran_data_row.item;

          CLOSE cr_item_supp;

          RAISE PROGRAM_ERROR;
        END IF;

        CLOSE cr_item_supp;

        -- get location number of the transfer to location
        IF cr_tran_data_row.tran_code IN (30, 32) THEN
          OPEN cr_tsf_to_loc (cr_tran_data_row.ref_no_1);

          FETCH cr_tsf_to_loc INTO L_tsf_to_loc;

          IF cr_tsf_to_loc%NOTFOUND THEN
            IO_error_message := 'Cursor cr_tsf_to_loc No data found Tsf_no:' || cr_tran_data_row.ref_no_1;

            CLOSE cr_tsf_to_loc;

            RAISE PROGRAM_ERROR;
          END IF;

          CLOSE cr_tsf_to_loc;
        ELSE
          L_tsf_to_loc := NULL;
        END IF;

        IF cr_tran_data_row.tran_code = 30 THEN
          IF cr_tran_data_row.ref_no_1 = I_tsf_in_no THEN
            L_cd_iftd_rowid.tsf_in_30 := L_iftd_rowid;
          ELSIF cr_tran_data_row.ref_no_1 = I_tsf_out_no THEN
            L_cd_iftd_rowid.tsf_out_30 := L_iftd_rowid;
          END IF;
        END IF;

        IF cr_tran_data_row.tran_code = 32 THEN
          IF cr_tran_data_row.ref_no_1 = I_tsf_in_no THEN
            L_cd_iftd_rowid.tsf_in_32 := L_iftd_rowid;
          ELSIF cr_tran_data_row.ref_no_1 = I_tsf_out_no THEN
            L_cd_iftd_rowid.tsf_out_32 := L_iftd_rowid;
          END IF;
        END IF;

        IF cr_tran_data_row.tran_code = 95 THEN
          L_cd_iftd_rowid.td_code_95 := L_iftd_rowid;
        END IF;

        INSERT INTO kcs.tran_data_history_cd (item,
                                              dept,
                                              class,
                                              subclass,
                                              pack_ind,
                                              loc_type,
                                              location,
                                              tran_date,
                                              post_date,
                                              tran_code,
                                              adj_code,
                                              units,
                                              total_cost,
                                              total_retail,
                                              ref_no_1,
                                              ref_no_2,
                                              gl_ref_no,
                                              old_unit_retail,
                                              new_unit_retail,
                                              pgm_name,
                                              sales_type,
                                              vat_rate,
                                              av_cost,
                                              ref_pack_no,
                                              iftd_rowid,
                                              tran_data_timestamp,
                                              supplier_id,
                                              to_loc)
             VALUES (cr_tran_data_row.item,
                     cr_tran_data_row.dept,
                     cr_tran_data_row.class,
                     cr_tran_data_row.subclass,
                     cr_tran_data_row.pack_ind,
                     cr_tran_data_row.loc_type,
                     cr_tran_data_row.location,
                     cr_tran_data_row.tran_date,
                     DECODE (SIGN (cr_tran_data_row.tran_date - Lg_last_eom_date), 1, cr_tran_data_row.tran_date, Lg_last_eom_date + 1),
                     cr_tran_data_row.tran_code,
                     cr_tran_data_row.adj_code,
                     cr_tran_data_row.units,
                     cr_tran_data_row.total_cost,
                     cr_tran_data_row.total_retail,
                     cr_tran_data_row.ref_no_1,
                     cr_tran_data_row.ref_no_2,
                     cr_tran_data_row.gl_ref_no,
                     cr_tran_data_row.old_unit_retail,
                     cr_tran_data_row.new_unit_retail,
                     cr_tran_data_row.pgm_name,
                     cr_tran_data_row.sales_type,
                     cr_tran_data_row.vat_rate,
                     cr_tran_data_row.av_cost,
                     cr_tran_data_row.ref_pack_no,
                     L_iftd_rowid,
                     cr_tran_data_row.timestamp,
                     L_supplier_id,
                     L_tsf_to_loc);
      END IF;

      -- delete from tran_data
      DELETE FROM tran_data
            WHERE CURRENT OF cr_tran_data;
    ---
    END LOOP;

    CLOSE cr_tran_data;

    -- fill kcs.tdhcd_tdh
    INSERT INTO kcs.tdhcd_tdh (iftd_rowid,
                               ref_tsf_no,
                               iftd_rowid_30,
                               iftd_rowid_32,
                               iftd_rowid_95)
      SELECT t.iftd_rowid,
             td.ref_tsf_no,
             td.iftd_rowid_30,
             td.iftd_rowid_32,
             td.iftd_rowid_95
        FROM rms.z_comm_tran_data_gtt t,
             (SELECT I_tsf_in_no AS ref_tsf_no,
                     L_cd_iftd_rowid.tsf_in_30 AS iftd_rowid_30,
                     L_cd_iftd_rowid.tsf_in_32 AS iftd_rowid_32,
                     L_cd_iftd_rowid.td_code_95 AS iftd_rowid_95
                FROM DUAL
              UNION ALL
              SELECT I_tsf_out_no,
                     L_cd_iftd_rowid.tsf_out_30,
                     L_cd_iftd_rowid.tsf_out_32,
                     L_cd_iftd_rowid.td_code_95
                FROM DUAL) td
       WHERE t.location = I_location
         AND t.loc_type = I_loc_type
         AND t.comm_il_sign_units = I_comm_il_sign_units;


    ---
    log_debug (0, c_ProgramName, 'FINISH');
    ---
    RETURN TRUE;
  ---
  EXCEPTION
    WHEN PROGRAM_ERROR THEN
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
    WHEN OTHERS THEN
      IO_error_message :=
        SQL_LIB.CREATE_MSG ('PACKAGE_ERROR',
                            SQLERRM,
                            c_ProgramName,
                            TO_CHAR (SQLCODE));
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
  END;

  ------------------------------------------------------------------------------------
  -- Public Functions: process
  ------------------------------------------------------------------------------------
  FUNCTION process (IO_error_message IN OUT VARCHAR2)
    RETURN BOOLEAN IS
    c_ProgramName   CONSTANT VARCHAR2 (60) := Cg_code_name || '.PROCESS';
    ---
    L_in_tsf_no              tsfhead.tsf_no%TYPE;
    L_out_tsf_no             tsfhead.tsf_no%TYPE;
    ---

  BEGIN
    ---
    log_debug (0, c_ProgramName, 'START');

    --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    -- initialization
    --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IF NOT init (IO_error_message) THEN
      RETURN FALSE;
    END IF;

    --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    -- get trascation data from if_tran_data
    --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IF NOT get_if_tran_data (IO_error_message) THEN
      RETURN FALSE;
    END IF;

    FOR cr_cd_loc IN (  SELECT t.location, t.loc_type, t.comm_il_sign_units
                          FROM rms.z_comm_tran_data_gtt t
                         WHERE t.comm_il_sign_units <> 0
                      GROUP BY t.location, t.loc_type, t.comm_il_sign_units) LOOP
      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      --1) Creation transfer in
      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

      IF cr_cd_loc.comm_il_sign_units > 0 THEN
        IF comm_tsf_cre (IO_error_message   => IO_error_message,
                         I_location         => cr_cd_loc.location,
                         I_loc_type         => cr_cd_loc.loc_type,
                         I_from_loc_type    => cr_cd_loc.loc_type,
                         I_from_loc         => cr_cd_loc.location,
                         I_to_loc_type      => 'W',
                         I_to_loc           => Lg_code_detail_wh6,
                         I_il_units_sign    => cr_cd_loc.comm_il_sign_units,
                         O_tsf_no           => L_in_tsf_no) = FALSE THEN
          RETURN FALSE;
        END IF;
      ELSE
        IF comm_tsf_cre (IO_error_message   => IO_error_message,
                         I_location         => cr_cd_loc.location,
                         I_loc_type         => cr_cd_loc.loc_type,
                         I_from_loc_type    => 'W',
                         I_from_loc         => Lg_code_detail_wh6,
                         I_to_loc_type      => cr_cd_loc.loc_type,
                         I_to_loc           => cr_cd_loc.location,
                         I_il_units_sign    => cr_cd_loc.comm_il_sign_units,
                         O_tsf_no           => L_in_tsf_no) = FALSE THEN
          RETURN FALSE;
        END IF;
      END IF;

      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      --2) Creation of shipment for trasfer in
      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      log_debug (0, 'RMS.SHIPMENT_SQL.SHIP_TSF:' || L_in_tsf_no, 'START');
      IF NOT rms.shipment_sql.ship_tsf (O_error_message => IO_error_message, I_tsf_no => L_in_tsf_no, I_pub_ind => 'N') THEN
        IO_error_message := 'RMS.SHIPMENT_SQL.SHIP_TSF - ' || IO_error_message;
        RAISE PROGRAM_ERROR;
      END IF;
      log_debug (0, 'RMS.SHIPMENT_SQL.SHIP_TSF:' || L_in_tsf_no, 'FINISH');

      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      --3) Receiving of Shipment for trasfer in
      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      IF NOT receive_comm_tsf (IO_error_message => IO_error_message, I_tsf_no => L_in_tsf_no) THEN
        RETURN FALSE;
      END IF;

      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      --4) Close trasfer in
      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      IF NOT close_comm_tsf (IO_error_message => IO_error_message, I_tsf_no => L_in_tsf_no) THEN
        RETURN FALSE;
      END IF;

      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      --5) Creation transfer out
      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

      IF cr_cd_loc.comm_il_sign_units > 0 THEN
        IF comm_tsf_cre (IO_error_message   => IO_error_message,
                         I_location         => cr_cd_loc.location,
                         I_loc_type         => cr_cd_loc.loc_type,
                         I_from_loc_type    => 'W',
                         I_from_loc         => Lg_code_detail_wh9,
                         I_to_loc_type      => cr_cd_loc.loc_type,
                         I_to_loc           => cr_cd_loc.location,
                         I_il_units_sign    => cr_cd_loc.comm_il_sign_units,
                         O_tsf_no           => L_out_tsf_no) = FALSE THEN
          RETURN FALSE;
        END IF;
      ELSE
        IF comm_tsf_cre (IO_error_message   => IO_error_message,
                         I_location         => cr_cd_loc.location,
                         I_loc_type         => cr_cd_loc.loc_type,
                         I_from_loc_type    => cr_cd_loc.loc_type,
                         I_from_loc         => cr_cd_loc.location,
                         I_to_loc_type      => 'W',
                         I_to_loc           => Lg_code_detail_wh9,
                         I_il_units_sign    => cr_cd_loc.comm_il_sign_units,
                         O_tsf_no           => L_out_tsf_no) = FALSE THEN
          RETURN FALSE;
        END IF;
      END IF;
      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      --6) Creation of shipment for trasfer out
      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      log_debug (0, 'RMS.SHIPMENT_SQL.SHIP_TSF:' || L_out_tsf_no, 'START');
      IF NOT rms.shipment_sql.ship_tsf (O_error_message => IO_error_message, I_tsf_no => L_out_tsf_no, I_pub_ind => 'N') THEN
        IO_error_message := 'RMS.SHIPMENT_SQL.SHIP_TSF - ' || IO_error_message;
        RAISE PROGRAM_ERROR;
      END IF;
      log_debug (0, 'RMS.SHIPMENT_SQL.SHIP_TSF:' || L_out_tsf_no, 'FINISH');

      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      --7) Receiving of Shipment for trasfer out
      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      IF NOT receive_comm_tsf (IO_error_message => IO_error_message, I_tsf_no => L_out_tsf_no) THEN
        RETURN FALSE;
      END IF;

      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      --8) Close trasfer out
      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      IF NOT close_comm_tsf (IO_error_message => IO_error_message, I_tsf_no => L_out_tsf_no) THEN
        RETURN FALSE;
      END IF;

      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      --9) wtite Custom transaction 'Commission inventory changes' to tran_data
      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      IF NOT write_tran_data (IO_error_message       => IO_error_message,
                              I_location             => cr_cd_loc.location,
                              I_loc_type             => cr_cd_loc.loc_type,
                              I_comm_il_sign_units   => cr_cd_loc.comm_il_sign_units) THEN
        RETURN FALSE;
      END IF;

      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      --10) move transactions from tran_data to if_tran_data based on transfer in and transfer out
      -- fill historical table KCS.TRAN_DATA_HISTORY_CD with custom 'Commission inventory changes' and virtual transfers transactions
      -- fill KCS.TDHCD_TDH with links to original commission transactions
      --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      IF NOT proc_cust_data (IO_error_message       => IO_error_message,
                             I_location             => cr_cd_loc.location,
                             I_loc_type             => cr_cd_loc.loc_type,
                             I_comm_il_sign_units   => cr_cd_loc.comm_il_sign_units,
                             I_tsf_in_no            => L_in_tsf_no,
                             I_tsf_out_no           => L_out_tsf_no) THEN
        RETURN FALSE;
      END IF;
    END LOOP;

    log_debug (0, c_ProgramName, 'FINISH');
    ---
    RETURN TRUE;
  ---
  EXCEPTION
    WHEN PROGRAM_ERROR THEN
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
    WHEN OTHERS THEN
      IO_error_message :=
        SQL_LIB.CREATE_MSG ('PACKAGE_ERROR',
                            SQLERRM,
                            c_ProgramName,
                            TO_CHAR (SQLCODE));
      log_debug (1, c_ProgramName, IO_error_message);
      RETURN FALSE;
  END;
--------------------------------------------------------------------------------
END;
/
