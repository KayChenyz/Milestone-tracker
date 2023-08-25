WITH latest_mpdate as (
SELECT [ProjectId]
      ,[PROJECT_SIZE_ID]
      ,[PROJECT_SIZE_NAME]
      ,CAST([CheckDate] as date) as ChangeDate
      ,RANK() OVER (PARTITION by ProjectId,PROJECT_SIZE_ID,PROJECT_SIZE_NAME ORDER by CheckDate DESC) as MPrnk 
      /*Rank checkdate to find latest one and it's MPdate */
      ,CAST([ChangedDate] as date) MPdate
      ,DATEADD(DAY, -7, CAST([ChangedDate] as date)) MP_CP_deadline
      
  FROM [PR_WEB2].[dbo].[T_MpDate_Log] mplog
  JOIN [PR_WEB2].[dbo].[T_PS_ProdSizeRelateInfo] info
  ON info.[PROJECT_ID] = mplog.[ProjectId]
  WHERE PROJECT_CATEGORY != 'OEM Auto'
  /* Since rnk can't be "whered" in above query, so I create a CTE to store query result temporary and do "where" below */
  ),
  PRclose_cp as(
    SELECT [T_PID_CkList].[PROJECT_SIZE_ID]
      ,[Category_Name]
      ,[Item_Name]
      ,[Status] AS MP_Status
      ,[UpdateUser]AS MP_UpdateUser
      ,[UpdateTime]AS MP_UpdateTime
      ,[T_PID_CkList].[Frequency]
      ,RANK() OVER (PARTITION by [T_PID_CkList].[PROJECT_SIZE_ID],[Category_Name],[Item_Name]ORDER by [T_PID_CkList].[Frequency] ASC) as MPrnk2
  FROM [PR_WEB2].[dbo].[T_PID_CkList]
  LEFT JOIN [T_PID_CkListFiles]
  ON ([T_PID_CkListFiles].[PROJECT_SIZE_ID] = [T_PID_CkList].[PROJECT_SIZE_ID]
  and [T_PID_CkListFiles].[MpType_ID] = [T_PID_CkList].[MpType_ID]
  and [T_PID_CkListFiles].[Category_ID] = [T_PID_CkList].[Category_ID]
  and [T_PID_CkListFiles].[Item_ID] = [T_PID_CkList].[Item_ID]
  and [T_PID_CkListFiles].[Frequency] = [T_PID_CkList].[Frequency]
  /* Frequency is duplicated phase count (e.g. Twice FP) */)
  where Item_Name = 'Control Plan' and Category_Name = 'Pilot Run Close'
  /* Pilot Run Close Control Plan is MP control plan*/
  )
  
  SELECT 
  [latest_mpdate].[PROJECT_SIZE_ID],
  [PROJECT_SIZE_NAME],
  [MPdate],
  [MP_CP_deadline],
  [MP_Status],
  [MP_UpdateTime],
  [MP_UpdateUser]
  FROM latest_mpdate
  JOIN PRclose_cp on PRclose_cp.PROJECT_SIZE_ID = latest_mpdate.PROJECT_SIZE_ID
  WHERE MPrnk=1 AND MPrnk2 = 1 AND MPdate > '2023-08-01'
  AND latest_mpdate.PROJECT_SIZE_ID not in (1166, 1109, 1122, 1124, 1125)
  ORDER BY MPdate
