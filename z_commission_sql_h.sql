CREATE OR REPLACE PACKAGE z_cs_commission_sql AS
--
-- $Revision: 1801 $:
-- $Author: escender.sary $:
-- $HeadURL: svn://svn.weigandt-consulting.com/konzum/retail/branches/retail-13.2-2d/db/rms/schema/packages/z_commission_sql_h.sql $:
-- $Id: z_commission_sql_h.sql 1801 2015-04-02 14:53:07Z escender.sary $:
--
-- Request #29733: [Commision sales]Automatic creation of 'virtual transfers' and custom transaction
  /*******************************************************************************************************************************************
  Object name:   z_cs_comission__sql
  Purpose:       contains components used in Comission Trade
  Remarks:

  MODIFICATION HISTORY (add new entries on top!)
  DD.MM.YY     Who                     Which program unit                  Version               What
  --------------------------------------------------------------------------------------------------------------------------------------------
  11.03.15     Sergey Fedoronchuk                                          v1.0                  new
  *******************************************************************************************************************************************/
  vg_Debug   BOOLEAN := FALSE;

  ---
  FUNCTION process (IO_error_message IN OUT VARCHAR2)
    RETURN BOOLEAN;
---
END;
/