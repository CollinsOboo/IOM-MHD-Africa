use MiMOSAUPGRADE;

declare @today date = getdate();
declare @peiom varchar(20) = 'MEDEXM002';

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
        -- for some countries the name is too long, truncate at the first comma (,)
        Case 
            when x.code = 'CD' then 'CONGO DRC'
            when x.code = 'CG' then 'CONGO BRA'
			when x.code = 'TZ' then 'TANZANIA'
            when charindex(',', x.Description)>0
            then left(x.Description, charindex(',', x.Description)-1) 
            else x.Description
        end CountryName,
        x.Code CountryCode
    from RegionCountry x
    where x.isRevoke<>1
        and x.Regions_Code in ('ZHBA01-DKR', 'ZHBA01-NBO', 'ZHBA01-PRY')
        -- Africa, change when using this query for other regions or globally
        --and x.regions_code in ('ZHBA01-NBO', 'ZHBA01-DKR', 'ZHBA01-PRY')
),

ac as (
    select
        x.CaseNo,
        x.Owner as BusinessArea,
		rc.Region,
        rc.CountryName OwnerCountry,
        x.ServiceCode,
        -- IMM or REF as per the program name.
        iif(x.ServiceCode like '%IMM', 'IMM', 'REF') MigrantType,
        -- sequence of ac per Case to remove duplicates
        row_number() over(partition by x.CaseNo order by x.startDate asc) Seq
    from Activity x
    inner join  rc on left(x.[Owner],2) = rc.CountryCode
    where x.IsRevoke<>1
        and x.ServiceCode not in ('GB_IMM','ACR_MED','UNHCR_MED', 'OTHER', 'OTHER_REF')
        and x.activityCode = 'MOV'
),

abns
as
(
select
    ac.CaseNo,
	ac.OwnerCountry,
    cm.CaseMemberID,
    ac.Region, ac.BusinessArea, ac.ServiceCode, mv.EmPDepartureDate,
    mc.PrimaryRefNo,mc.Category,mc.Location,mc.LocationCountry,
		case 
			when mc.LocationCountry = 'CD' then 'CONGO DRC'
			when mc.LocationCountry = 'CG' then 'CONGO BRA'
			when mc.LocationCountry = 'TZ' then 'TANZANIA'
			when charindex(',', Ctry.Description)>0
			then left(Ctry.Description, charindex(',', Ctry.Description)-1) 
			else Ctry.Description
		end loccountry,
	nat.Description as Nationality, cob.Description as BirthCountry,
    (select count (1) from CaseMember cm where cm.CaseNo=ac.CaseNo and cm.IsRevoke=0) FamilySize, cm.SequenceNo as Seq,
    cm.AlienNo, cm.MemberNo, cm.LastName,cm.FirstName, cm.MiddleName, cm.RelationToPrimaryApplicant [Rel To PA],  cm.Gender,
    replace(convert(char(11),cm.Birthdate,113),' ','-') Birthdate,p.PhoneNumber, addr.Addressline1, addr.Addressline2,
    datediff(day,cm.BirthDate,mv.EmPDepartureDate)/365.25 as AgeYrsDep,
    datediff(day,cm.BirthDate,@today)/365.25 as AgeYrstoday, --Current Age
	mv.CreatedDate as MVCreatedDate, mv.DepartureCountry, mv.PFNo, mv.Status,
    replace(convert(char(11),mv.EmPDepartureDate,113),' ','-') DepartureDate, 
    replace(convert(char(11),mv.PoEArrivalDate,113),' ','-') ArrivalDate, mv.EmbarkationPort, mv.EntryPort, mv.DestinationCountry, dctry.Description DestCountry, abnc.ReasonforCancellation,
	concat('https://gvamimosaweb03.iom.int/mweb/Medical/Individual?caseMemberId=',cm.CaseMemberID) url
from
    ac
	inner join MigrantCase as mc on mc.CaseNo = ac.CaseNo
	inner join CaseMember as cm on ac.CaseNo = cm.CaseNo
	inner join MovementCases mvc on mvc.CaseNo = cm.CaseNo
	left join Movement mv on mv.PFNo = mvc.PFNo
	left join ABNCase abnc on cm.CaseNo = abnc.CaseNo
	left join Country ctry on mc.LocationCountry = ctry.Code
	left join Country nat on cm.Nationality = nat.Code
	left join Country cob on cm.BirthCountry = cob.Code
	left join Country dctry on dctry.Code = mv.DestinationCountry
	left join CaseMemberAddress Addr on addr.CaseMemberID = cm.CaseMemberID 
	outer apply (
		select 
			PhoneNumber = dbo.GROUP_CONCAT_D(ph.PhoneNo, ', ') 
		from 
			CaseMemberPhone ph    
		where 
			ph.CaseMemberID = cm.CaseMemberID  
		group by 
			ph.CaseMemberID 
	) p

--where mc.ManagingMission in (ac.BusinessArea)
where mc.LocationCountry in ('AO', 'BI', 'BW', 'CM', 'CF', 'TD', 'CG', 'CI', 'DJ', 'ER', 'ET', 'GA', 'GH', 'GN', 'KE', 'MG', 'MW', 'MR', 'MZ', 'NA', 'NG', 'NE', 'SN', 'SS', 'SO','TD','TG', 'TZ', 'UG', 'ZA', 'ZM', 'ZW', 'SD', 'CD', 'RW', 'GM') and (mv.EmPDepartureDate >= convert(datetime, '2020-01-01') OR mv.EmPDepartureDate IS NULL)-- AND mv. <> 'DEP'
),

has
as
(
select
	row_number() over (partition by abns.CaseMemberID order by cfa.CreatedDate desc) as farnk,
	abns.CaseMemberID,
    cfa.CaseMemberFunctionalAreaID,
    Case 
        when cfa.Description = 'Health Assessment' then 'HA-1'
        else concat('HA-', substring(cfa.Description, 19, 2)) 
    End as HACountShort,
	cfa.ExamDate,
    datediff(day,abns.BirthDate,cms.CompletionDate)/365.25 as AgeYrsComp,--Age @ PE
    replace(convert(char(11),cfa.ExamDate,113),' ','-') [PE Date], cfa.Status as MedStatus, 
    replace(convert(char(11),cfa.LastDatemodified,113),' ','-') MedStatusDate, pdms.Status as PDMSStatus
from
	abns
	inner join CaseMemberFunctionalArea as cfa on cfa.CaseMemberID = abns.CaseMemberID 
												 and cfa.FunctionalAreaCode = 'HA'
	inner join CaseMemberMedicalService as cms on cfa.CaseMemberID = cms.CaseMemberID 
												 and cfa.CaseMemberFunctionalAreaID = cms.CaseMemberFunctionalAreaID and cms.MedServiceCode = @peiom
	left join CaseMemberFunctionalArea as pdms on pdms.CaseMemberID = abns.CaseMemberID and pdms.FunctionalAreaCode = 'PDMS' 

),

abns_rnk
as
(
select abns.*, has.HACountShort, has.ExamDate, has.AgeYrsComp, has.[PE Date], has.MedStatus, has.MedStatusDate, has.PDMSStatus, row_number() over (partition by abns.CaseMemberID order by abns.MVCreatedDate desc) as rnk
from abns
left join has on has.CaseMemberID = abns.CaseMemberID and has.farnk = 1
)

select * from abns_rnk where rnk = 1
order by
	PrimaryRefNo,
	Seq