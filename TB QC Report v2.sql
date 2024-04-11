use mimosaupgrade;

declare @today date = getdate();
declare @panelSiteFieldNo varchar(20) = 'PH1A10079242326315';
declare @smearFinalResultFieldNo varchar(20) = 'PH1A10079242326150';
declare @cultureFinalResultFieldNo varchar(20) = 'PH1A10079242326144';
declare @class_combo varchar(20) = 'PH1A10079242326117'; --Form-based programs FieldNo
declare @class_view varchar(20) = 'PH1A10079242326165'; -- Non-form-based FieldNo
declare @peiom varchar(20) = 'MEDEXM002';
declare @collected1 varchar(20) = 'PH1A10079242326383';
declare @collected2 varchar(20) = 'PH1A10079242326384';
declare @collected3 varchar(20) = 'PH1A10079242326385';
declare @reported1 varchar(20) = 'PH1A10079242326380';
declare @reported2 varchar(20) = 'PH1A10079242326381';
declare @reported3 varchar(20) = 'PH1A10079242326382';
declare @smear varchar(20) = 'MEDMCR004';
declare @culture varchar(20) = 'MEDMCR003';
declare @mtbrif varchar(20) = 'MEDLAB113';
declare @mtbrifultra varchar(20) = 'MEDLAB114';
declare @dst varchar(20) = 'MEDMCR007';
declare @vaccines varchar(20) = 'MEDIMN%';

with 
rc as (
    select
        Case
            when x.regions_code = 'ZHBA01-DKR'
            then 'WCA'
            when x.regions_code = 'ZHBA01-NBO'
            then 'EHoA'
            when x.regions_code = 'ZHBA01-PRY'
            then 'SA'
        end Region,
        right(x.regions_code,3) RegionalOffice,
        -- for some countries the name is too long, truncate as the first comme (,)
        Case 
            when x.code = 'CD' then 'CONGO DRC'
            when x.code = 'CG' then 'CONGO BRA'
            when charindex(',', x.Description)>0
            then left(x.Description, charindex(',', x.Description)-1) 
            else x.Description
        end CountryName,
        x.Code CountryCode
    from Africa.RegionCountry x
    where x.isRevoke<>1
        and x.Regions_Code like 'ZHBA01-%'
        -- Africa, change when using this query for other regions or globally
        --and x.regions_code in ('ZHBA01-NBO', 'ZHBA01-DKR', 'ZHBA01-PRY')
),
ac as (
    select
        x.CaseNo,
        x.Owner,
		rc.Region,
        rc.CountryName OwnerCountry,
        x.ServiceCode,
        --x.activityCode,
        -- IMM or REF as per the program name.
        iif(x.ServiceCode like '%IMM', 'IMM', 'REF') MigrantType,
        -- sequence of ac per Case to remove duplicates
        row_number() over(partition by x.CaseNo order by x.startDate asc) seq
    from Africa.Activity x
    inner join  rc on left(x.[Owner],2) = rc.CountryCode
    where x.IsRevoke<>1
        --and x.ServiceCode not in ('GB_IMM','ACR_MED','UNHCR_MED', 'OTHER', 'OTHER_REF')
        and x.activityCode = 'MED'
),
-- ms: each medical service have a reference start date msStartDate which is
-- completion date otherwise appointment date otherwise last updated date, finally Created date
-- for status not CPL date date is null.
ms as (
    select 
        x.*,
        convert(date,coalesce(
            -- if smear of culture: start date is fisrt colection date, or appointment date
            case 
                when x.MedserviceCode in (@smear, @culture)
                then coalesce(
                    c.collectedDate,
                    convert(date,x.startDate)
                )
                else null
            end,
            --any other service, start date is completion date, or appointment date
            --or last update date, or created date
            case 
                when year(x.CompletionDate) > 1900 
                then convert(date,x.CompletionDate)
                else null 
            end,
            case 
                when year(x.startDate) > 1900 
                then convert(date,x.startDate)
                else null 
            end,
            case 
                when year(x.LastDatemodified) > 1900 
                then convert(date,x.LastDatemodified)
                else null 
            end,
            case 
                when year(x.CreatedDate) > 1900 
                then convert(date,x.CreatedDate)
                else null 
            end
        )) msStartDate,
        -- reported date for smear and culture, or completion date
        convert(date,coalesce(
            case 
                when x.MedserviceCode in (@smear, @culture)
                then r.reportedDate
                else null
            end,
            -- any other service, completion date
            case 
                when year(x.CompletionDate) > 1900 
                then convert(date,x.CompletionDate)
                else null 
            end
        )) msReportedDate
    from Africa.CaseMemberMedicalService x
    left join (
        -- eariest sample date
        select 
            CaseMemberMedServiceID,
            min(convert(date,[Value])) collectedDate
            --min([Value]) collectedDate
        from Africa.CaseMemberMedServiceOtherInfo
        where FieldNo in (@collected1, @collected2, @collected3)
            and [value] <> '__-___-____'
            and [value] like '__-___-____'
        group by CaseMemberMedServiceID

    ) c
    on c.CaseMemberMedServiceID = x.CaseMemberMedicalServiceID
    left join (
        -- eariest sample date
        select 
            CaseMemberMedServiceID,
            min(convert(date,[Value])) reportedDate
            --min([Value]) reportedDate
        from Africa.CaseMemberMedServiceOtherInfo
        where FieldNo in (@reported1, @reported2, @reported3)
            and [value] <> '__-___-____'
            and [value] like '__-___-____'
        group by CaseMemberMedServiceID

    ) r
    on r.CaseMemberMedServiceID = x.CaseMemberMedicalServiceID
    where x.MedServiceCode not in ('MEDLAB001','MEDLAB071','MEDEXM142') --excluding IGRA and HBsAg
),

--tbcf: Obtains TB OutcomeClassification from custom fields not CFA
tbcf as (
		select
			Row_Number() OVER (PARTITION BY oi.casemembermedserviceid ORDER BY oi.lastdatemodified DESC) as rnk,
			cmms.casememberfunctionalareaid,
			oiv.value as VOutcomeClass,
			oic.value as COutcomeClass,
			case
				when act.servicecode in ('AUS_IMM','AUS_REF','CAN_REF','CAN_IMM','OTHER') and oic.value <>''
				then oic.value
				when act.servicecode in ('AUS_IMM','AUS_REF','CAN_REF','CAN_IMM','OTHER') and (oic.value ='' and oiv.value <>'')
				then oiv.value
				when act.servicecode not in ('AUS_IMM','AUS_REF','CAN_REF','CAN_IMM','OTHER') and oiv.value <>''
				then oiv.value
				when act.servicecode not in ('AUS_IMM','AUS_REF','CAN_REF','CAN_IMM','OTHER') and (oiv.value ='' and oic.value <>'')
				then oic.value
			end ROutcomeClassification
			from
				africa.casemembermedicalservice cmms
				inner join
				africa.CaseMemberMedServiceOtherInfo oi with (nolock) on cmms.casemembermedicalserviceid = oi.casemembermedserviceid and oi.FieldNo IN (@class_combo, @class_view)
				left join
				africa.CaseMemberMedServiceOtherInfo oic with (nolock) on cmms.casemembermedicalserviceid = oic.casemembermedserviceid and oic.FieldNo = @class_combo
				left join
				africa.CaseMemberMedServiceOtherInfo oiv with (nolock) on cmms.casemembermedicalserviceid = oiv.casemembermedserviceid and oiv.FieldNo = @class_view
				left join
				africa.casemember cm with (nolock) on cm.casememberid = cmms.casememberid
				left join
				ac act with (nolock) on act.caseno = cm.caseno
),


--fa: determining the start and end dates for each FA:
-- start date is the start date of the first medical service and end date is the last medical service.
fa as (
    select 
        x.*,
        y.FAStartDate,
        y.FAEndDate,
		t.ROutcomeClassification,
		case
			when
			t.ROutcomeClassification like '%Class A TB%' -- US cases
			or t.ROutcomeClassification like '%Class B1 TB, Extrapulmonary%'
			or t.ROutcomeClassification like '%TB Active%' -- old IOM Form
			or t.ROutcomeClassification like '%TB[124]%' -- new IOM Form
			then 1
			else 0
		end TBClass
    from Africa.CaseMemberFunctionalArea x
    left join (
        select
            ms.CaseMemberFunctionalAreaID,
            min(ms.msStartDate) FAStartDate,
            max(ms.msStartDate) FAEndDate
        from ms
        where MedServiceCode not like @vaccines --exclude vaccines
        group by ms.CaseMemberFunctionalAreaID
        ) y
        on y.CaseMemberFunctionalAreaID = x.CaseMemberFunctionalAreaID
		left join tbcf t on t.casememberfunctionalareaid = x.casememberfunctionalareaid and t.rnk = 1

),

-- TB Diagnosis based on medical services results or nature
tbms as (
    select
        fa.CaseMemberID,
        fa.CaseMemberFunctionalAreaID,
        fa.functionalAreaCode,
        fa.Description,
        fa.CreatedDate,
        ms.msStartDate,
        ms.msReportedDate,
        fa.FAStartDate,
        fa.examDate,
        
        case 
            when fa.FunctionalAreaCode = 'TBRX'
            then ''
            when ms.MedServiceCode = @peiom and fa.TBClass>0
            then 'Active TB Classification is assigned'
            when ms.MedServiceCode in (@smear, @culture)
            then ms.MedServiceDescription+' is positive'
            when ms.MedServiceCode in (@mtbrif, @mtbrifultra)
            then ms.MedServiceDescription+' is positive'
            else ms.MedServiceDescription+' is present'
        end reason,
        
        case -- TBRX
            when fa.FunctionalAreaCode = 'TBRX'
            then 1
            else 0
        end tbrx,
        case -- classification: using peiom to capture only once
            when fa.FunctionalAreaCode <> 'TBRX' 
            and ms.MedServiceCode = @peiom 
            and fa.TBClass>0
            then 1
            else 0
        end TBClass,
        case --smear positive
            when fa.FunctionalAreaCode <> 'TBRX' 
            and ms.MedServiceCode = @smear 
            and oi.Value like 'POS%'
            then 1 else 0
        end smear,
        case --culture positive
            when fa.FunctionalAreaCode <> 'TBRX' 
            and ms.MedServiceCode = @culture 
            and oi.Value like 'POS%'
            then 1 else 0
        end culture,
        case --GEneXpert positive
            when fa.FunctionalAreaCode <> 'TBRX' 
            and ms.MedServiceCode in (@mtbrif, @mtbrifultra)
            and ms.Result in (
                'MTBTRCDET', -- MTB Trace detected
                'MTBTTRACEDRIFIND00', -- MTB Trace detected/RIF indeterminate
                'MTBTTRACEDRIFRES00', -- MTB Trace detected/RIF resistant
                'MTBTTRACEDRIFSUS00', -- MTB Trace detected/RIF susceptible
                'RMTBRIFINCONCLUSIV', -- MTB/RIF Inconclusive
                'RMTBRIFRESISTANT00', -- MTB/RIF resistant
                'RMTBRIFSENSITIVE00'--, -- MTB/RIF susceptible
                )
            then 1 else 0
        end mtbrif,
        case --DST present
            when fa.FunctionalAreaCode <> 'TBRX' 
            and ms.MedserviceCode = @dst 
            then 1 else 0
        end dst,
        case --RX present
            when fa.FunctionalAreaCode <> 'TBRX'
            and ms.MedServiceCode in (
                'MEDTHR049',-- Hospitalisation - In patient TB 
                'MEDTHR050',-- Hospitalisation - Out patient TB 
                'MEDTHR036',-- TB Rx 
                'MEDTHR008',-- TB Rx IOM 
                'MEDTHR033',-- TB Rx Non-IOM 
                'MEDTHR014',-- TB Rx Curative 2HRZE 
                'MEDTHR015',-- TB Rx Curative 4HR
                'MEDEXM117',-- PE TB Entry 
                'MEDEXM118',-- PE TB Follow-up 
                'MEDEXM119',-- PE TB Exit								 
                'MEDTHR009',-- Th - TB - MDR 
                'MEDTHR018',-- Isoniazid (TB2HRZE) 
                'MEDTHR019'-- Rifampicin (TB2HRZE) 
                )
            then 1 else 0
        end rx
 
    from ms
    left join Africa.CaseMemberMedServiceOtherInfo oi
        on oi.CaseMemberMedServiceID = ms.CaseMemberMedicalServiceID
        and oi.FieldNo in (@smearFinalResultFieldNo, @cultureFinalResultFieldNo)
    left join fa
        on ms.CaseMemberFunctionalAreaID = fa.CaseMemberFunctionalAreaID
    where 
        --fa.FunctionalAreaCode <> 'TBRX'
        --and 
        ms.status <>'NON'
        and 
        (
        --classification as TB
        (ms.MedServiceCode = @peiom and fa.TBClass>0 )
        --smear positive
            or (ms.MedServiceCode = @smear and oi.Value like 'POS%')
        --culture positive
            or (ms.MedServiceCode = @culture and oi.Value like 'POS%')
        -- GeneXpert positive
            or (ms.MedServiceCode in (@mtbrif, @mtbrifultra)
                and ms.Result in (
                    'MTBTRCDET', -- MTB Trace detected
                    'MTBTTRACEDRIFIND00', -- MTB Trace detected/RIF indeterminate
                    'MTBTTRACEDRIFRES00', -- MTB Trace detected/RIF resistant
                    'MTBTTRACEDRIFSUS00', -- MTB Trace detected/RIF susceptible
                    'RMTBRIFINCONCLUSIV', -- MTB/RIF Inconclusive
                    'RMTBRIFRESISTANT00', -- MTB/RIF resistant
                    'RMTBRIFSENSITIVE00'--, -- MTB/RIF susceptible
                    )
            )
        -- DST present, RX present
            or ms.MedServiceCode in (
                    @dst,
                    'MEDTHR049',-- Hospitalisation - In patient TB 
                    'MEDTHR050',-- Hospitalisation - Out patient TB 
                    'MEDTHR036',-- TB Rx 
                    'MEDTHR008',-- TB Rx IOM 
                    'MEDTHR033',-- TB Rx Non-IOM 
                    'MEDTHR014',-- TB Rx Curative 2HRZE 
                    'MEDTHR015',-- TB Rx Curative 4HR 							 
                    'MEDEXM117',-- PE TB Entry 
                    'MEDEXM118',-- PE TB Follow-up 
                    'MEDEXM119',-- PE TB Exit								 
                    'MEDTHR009',-- Th - TB - MDR 
                    'MEDTHR018',-- Isoniazid (TB2HRZE) 
                    'MEDTHR019'-- Rifampicin (TB2HRZE) 
                    )
        )
),
-- grouping ms to fa
tbfa0 as (
    select 
        *,
        -- calculate episod of TB, assign parentID to FA of same TB episod
        count(case when z.TBDxInterval>6 then 1 else null end) 
            over(partition by z.CaseMemberID order by z.TBDiagnosisDate asc
                rows between unbounded preceding and current row) tb_episod,
        -- assign temporary seq number for TBRX so it can be excluded in some window functions later
        case 
            when z.functionalAreaCode = 'TBRX'
            then 1000000
            else 1
        end _temp_seq,
        -- associate each TB FA with its parent FA: SE, SF. TBRX and WRX cant be parents
        case 
            -- if only one tbfa present, parent is itself
            when (count(1) over(partition by z.CaseMemberID))=1
            then z.CaseMemberFunctionalAreaID
            -- if tbfa is HA, parent is itself
            when z.functionalAreaCode = 'HA'
            then z.CaseMemberFunctionalAreaID
            else coalesce(
                (-- parentFA is the previous HA by examdate
                    select
                        top 1 p.CaseMemberFunctionalAreaID
                    from fa p
                    where p.CasememberId = z.Casememberid
                        and p.functionalAreaCode = 'HA'
                        and p.examDate<=z.examDate
                    order by p.examDate desc
                ),
                (-- parentFA is the previous HA by startdate
                    select
                        top 1 p.CaseMemberFunctionalAreaID
                    from fa p
                    where p.CasememberId = z.Casememberid
                        and p.functionalAreaCode = 'HA'
                        and p.FAStartDate<=z.FAStartdate
                    order by p.FAStartDate desc 
                ),
                (-- otherwise parentFA is the previous HA by Createddate
                    select
                    top 1 p.CaseMemberFunctionalAreaID
                    from fa p
                    where p.CasememberId = z.Casememberid
                        and p.functionalAreaCode = 'HA'
                        and p.CreatedDate<=z.CreatedDate
                    order by p.CreatedDate desc
                ),
                -- else parent FA is itself
                z.CaseMemberFunctionalAreaID                
            )
        end parentID
    from (
        select 
            *,
            -- calculate interval months between current TB FA and the previous one
            coalesce(
                datediff(month,
                    lag(y.TBDiagnosisDate) over (partition by y.CaseMemberID order by y.TBDiagnosisDate asc),
                    y.TBDiagnosisDate
                ),0
            ) TBDxInterval
        from (
            select 
                x.CaseMemberID,      
                x.CaseMemberFunctionalAreaID,
                x.FunctionalAreaCode,
                x.Description,
                min(x.CreatedDate) CreatedDate,
                min(x.msStartDate) TBDiagnosisDate,
                min(x.msReportedDate) TBReportedDate,
                max(x.msReportedDate) TBReportedDate2,
                min(x.FAStartDate) FAStartDate,
                min(x.examDate) examDate,
                
                case 
                    when x.FunctionalAreaCode='TBRX'
                    then '['+x.Description+': '+ 'TB Treatment module (TBRX) is present]'
                    else '['+x.Description+': '+ dbo.GROUP_CONCAT_D(reason, ', ')+']'
                end reason,
                sum(x.TBClass) TBClass,
                sum(x.smear) smear,
                sum(x.culture) culture,
                sum(x.mtbrif) mtbrif,
                sum(x.dst) dst,
                sum(x.rx) rx,
                sum(x.tbrx) tbrx,
                sum(x.TBClass)+sum(x.smear)+sum(x.culture)+sum(x.dst)+sum(x.mtbrif)+sum(x.rx)+sum(x.tbrx) TBScore
            from tbms x 
            group by x.CaseMemberId, x.CaseMemberFunctionalAreaID, x.FunctionalAreaCode, x.Description
        ) y
    ) z
),

tbfa as (
    select 
        x.CaseMemberID,      
        x.CaseMemberFunctionalAreaID,
        x.FunctionalAreaCode,
        x.Description,
        x.CreatedDate,
        x.TBDiagnosisDate,
        x.TBReportedDate,
        x.TBReportedDate2,
        x.FAStartDate,
        x.examDate,
        x.reason,
        x.TBClass,
        x.smear,
        x.culture,
        x.mtbrif,
        x.dst,
        x.rx,
        x.tbrx,
        x.TBScore,
        dbo.GROUP_CONCAT_D(case when x.functionalAreaCode='TBRX' then x.CaseMemberFunctionalAreaID else null end,', ')
            over(partition by x.CaseMemberID) ALL_TBRX_IDs,
        dbo.GROUP_CONCAT_D(case when x.functionalAreaCode='TBRX' then x.Description else null end,', ')
            over(partition by x.CaseMemberID) ALL_TBRX_Descriptions,
        stuff(
            IIF(x.TBClass>0 and (x.smear+x.culture+x.mtbrif+x.dst)=0,', ['+x.Description+': TB Class without microbiology diagnosis, Either Clinical Diagnisis or false TB Class]', '')+
            IIF(x.culture>0 and x.dst=0,', ['+x.Description+': culture is positive but DST not Done]', '')+
            IIF(x.culture=0 and x.dst>0,', ['+x.Description+': DST done however Culture is not done or is negative or result not entered]', '')+
            IIF(x.rx>0 and (x.TBClass+x.smear+x.culture+x.mtbrif)=0,', ['+x.Description+': TB treatment services without TB Class or Laboratory diagnosis]', '')+
            IIF(x.FunctionalAreaCode = 'WRX', ', ['+x.Description+': TB Diagnosis in WRX not expected, WRX could be used erronously instead of TBRX module]', '')
        ,1,2,''
        ) QCHints,
        -- reassign parent FA for group of tbfa in same TB episod to the tfa highest score
        -- and assign parent TB FA for TBRX
        case
            -- parent from tbfa
            when x.FunctionalAreaCode='TBRX'
            then coalesce(
                    -- single FA + TBRX, parent is FA
                    case 
                        when (count(1) over(partition by x.CaseMemberID))=2
                        then first_value(x.parentID2)over(partition by x.CaseMemberID order by x._temp_seq asc)
                    else null
                    end,
                    -- parentFA is the parent of previous TB FA by diagnosis date
                    last_value(x.parentID2) over(partition by x.CaseMemberID order by x.TBDiagnosisDate asc
                    rows between unbounded preceding and current row),
                    -- parentFA is the parent of previous TB FA by start date
                    last_value(x.parentID2) over(partition by x.CaseMemberID order by x.FAStartdate asc
                    rows between unbounded preceding and current row),
                    -- parentFA is the parent of previous TB FA by creation date
                    last_value(x.parentID2) over(partition by x.CaseMemberID order by x.CreatedDate asc
                    rows between unbounded preceding and current row),
                    -- parentFA is the parent of any (first) TB FA by creation date
                    last_value(x.parentID2) over(partition by x.CaseMemberID order by x.TBDiagnosisDate asc),
                    --else initial parent.
                    x.parentID2                
                )
            -- for non TBRX FAs keep initial parent
            else x.parentID2
        end parentID
    from (
        select 
            *,
            first_value(parentID) over(partition by CaseMemberID, tb_episod order by TBSCore desc) parentID2
        from tbfa0
    ) x
),
-- grouping TB FAs by parent FA
tb as (
    select 
        x.CasememberId,      
        x.parentID CaseMemberFunctionalAreaID,
        dbo.GROUP_CONCAT_D(x.CaseMemberFunctionalAreaID,',') CaseMemberFunctionalAreaIDs,
        dbo.GROUP_CONCAT_D(x.FunctionalAreaCode,',') FunctionalAreaCodes,
        dbo.GROUP_CONCAT_D(x.Description,',') Descriptions,
        min(x.FAStartDate) FAStartDate,
        min(x.CreatedDate) CreatedDate,
        min(x.TBDiagnosisDate) TBDiagnosisDate,
        min(x.TBReportedDate) TBReportedDate,
        max(x.TBReportedDate2) TBReportedDate2,
        dbo.GROUP_CONCAT_D(x.reason, ', ') reasons,
        dbo.GROUP_CONCAT_D(x.QCHints, ', ') QCHints,
        sum(x.TBClass) TBClass,
        sum(x.smear) smear,
        sum(x.culture) culture,
        sum(x.dst) dst,
        sum(x.mtbrif) mtbrif,
        sum(x.rx) rx,
        sum(x.tbrx) tbrx,
        sum(x.TBScore) TBScore,
        dbo.GROUP_CONCAT_D(case when x.tbrx>0 then x.CaseMemberFunctionalAreaID end, ', ') TBRX_ID,
        dbo.GROUP_CONCAT_D(case when x.tbrx>0 then x.Description end, ', ') TBRXDescription,
        min(x.ALL_TBRX_IDs) ALL_TBRX_IDs,
        min(x.ALL_TBRX_Descriptions) ALL_TBRX_Descriptions
        
    from tbfa x 
    group by x.CasememberId,x.parentID
),

dataset as (
    select 
        ac.Owner,
		ac.Region,
        ac.OwnerCountry,
        ac.ServiceCode,
        dest.CountryName DestinationCountry,
        loc.CountryName LocationCountry,
        mc.Location,
        mc.CaseNo,
        mc.PrimaryRefNo,

        cm.CaseMemberID,
        cm.MemberNo,
        cm.RelationToPrimaryApplicant,
        case 
            when cm.middleName is not null and cm.middleName<>''
            then concat(cm.LastName,', ',cm.FirstName, ' ', cm.middleName) 
            else concat(cm.LastName,', ',cm.FirstName) 
        end FullName,
        cm.LastName,
        cm.FirstName,
        cm.MiddleName,
        cm.Gender,
        cm.BirthDate,
        cast(DATEDIFF(day, cm.BirthDate, fa.CreatedDate) as float)/365.25 PEAge,
        cast(DATEDIFF(day, cm.BirthDate, @today) as float)/365.25 CurrentAge,
        nat.CountryName Nationality,
        tb.reasons,
        stuff(
            ', '+tb.QCHints +
            IIF(fa.FunctionalAreaCode = 'HA' and ac.ServiceCode not in ('AUS_IMM','AUS_REF','CAN_IMM','CAN_REF') 
            and tb.TBClass=0 and (tb.smear+tb.culture+tb.mtbrif)>0,', ['+fa.Description+': TB microbiology diagnosis without TB Class in HA]', '')
            -- to be added when new Class A SOP is activated.
            --IIF(tb.rx>1, ' and TB Treatment Services assigned in '+x.description+' while it should be in TBRx', '')+
        ,1,2,''
        ) QCHints,
        tb.TBClass,
        tb.smear,
        tb.culture,
        tb.mtbrif,
        tb.dst,
        tb.rx,
        tb.tbrx,
        tb.TBScore,
        fa.CaseMemberFunctionalAreaID,
        fa.functionalAreaCode,
        fa.Description,
        fa.Status,
        fa.ROutcomeClassification as OutcomeClassification,
        tb.TBRX_ID,
        tb.TBRXDescription,
        tb.ALL_TBRX_IDs,
        tb.ALL_TBRX_Descriptions,
        tb.CaseMemberFunctionalAreaIDs,
        tb.FunctionalAreaCodes,
        tb.Descriptions,
        tb.TBDiagnosisDate,
        tb.TBReportedDate,
        tb.TBReportedDate2,
        fa.FAstartDate,
        fa.ExamDate,
        fa.FAEndDate,
        fa.CompletionDate,        
        (   
            select top 1 oi.Value
            from Africa.CaseMemberMedicalService ms
            left join Africa.CaseMemberMedServiceOtherInfo oi
                on oi.CaseMemberMedServiceID = ms.CaseMemberMedicalServiceID
                and oi.FieldNo = @panelSiteFieldNo
            where ms.CaseMemberFunctionalAreaID = fa.CaseMemberFunctionalAreaID
                and ms.medServiceCode = (
                select top 1 mps.MedServiceCode
                from Africa.MedProfileSetup mps
                where mps.ProfileCode = fa.ProfileCode
                    and mps.PrimaryService=1
            )
            order by ms.CreatedDate asc
        ) PanelSite,
        (   
            select top 1 sp.Description
            from Africa.CaseMemberMedicalService ms
            left join Africa.ServiceProvider sp 
                on sp.ServiceProviderCode = ms.MedServiceProviderCode
            where ms.CaseMemberFunctionalAreaID = fa.CaseMemberFunctionalAreaID
                and ms.medServiceCode = (
                select top 1 mps.MedServiceCode
                from Africa.MedProfileSetup mps
                where mps.ProfileCode = fa.ProfileCode
                    and mps.PrimaryService=1
            )
            order by ms.CreatedDate asc
        ) ServiceProvider

    from tb 
    left join fa 
        on fa.CaseMemberFunctionalAreaID = tb.CaseMemberFunctionalAreaID
    left join Africa.CaseMember cm 
        on cm.CaseMemberID = tb.CaseMemberID
    left join Africa.MigrantCase mc 
        on mc.CaseNo = cm.CaseNo
    left join ac 
        on ac.CaseNo = cm.CaseNo
    left join rc dest
        on mc.DestinationCountry = dest.CountryCode
    left join rc loc 
        on mc.LocationCountry = loc.CountryCode
    left join rc nat
        on cm.nationality = nat.CountryCode
    where ac.seq=1
        and mc.isrevoke<>1
        and cm.isrevoke<>1
		--ID and eliminate Australia cases with only dst as the TB indicator
        --and not (tb.TBScore=tb.dst and left(ac.ServiceCode,3) = 'AUS')
)

select * 
from dataset
order by 
    owner, CaseNo, CaseMemberID, currentAge desc, TBDiagnosisDate desc;