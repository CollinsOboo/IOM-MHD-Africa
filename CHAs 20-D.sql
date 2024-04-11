use MiMOSAUPGRADE;

declare @today date = getdate();
declare @panelSiteFieldNo varchar(20) = 'PH1A10079242326315';
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
        and x.activityCode = 'MED'
),

has
as
(
select
    ac.CaseNo,
	ac.OwnerCountry,
    cm.CaseMemberID,
    cfa.CaseMemberFunctionalAreaID,
    Case 
        when cfa.Description = 'Health Assessment' then 'HA-1'
        else concat('HA-', substring(cfa.Description, 19, 2)) 
    End as HACountShort,
	concat('https://gvamimosaweb03.iom.int/mweb/Medical/Individual?caseMemberId=',cm.CaseMemberID) url,
    replace(convert(char(11),cfa.CreatedDate,113),' ','-') CFACreatedDate, ac.Region, ac.BusinessArea, ac.ServiceCode, sp.Description as ServiceProvider, o.Value as PanelSite, cfa.ExamDate , mv.EmPDepartureDate,
    mc.PrimaryRefNo,mc.Category,mc.Location,mc.LocationCountry,
	Case 
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
    datediff(day,cm.BirthDate,cms.CompletionDate)/365.25 as AgeYrsComp,--Age @ PE
    datediff(day,cm.BirthDate,@today)/365.25 as AgeYrstoday, --Current Age
    replace(convert(char(11),cfa.ExamDate,113),' ','-') [PE Date], cfa.Status as MedStatus, 
    replace(convert(char(11),cfa.LastDatemodified,113),' ','-') MedStatusDate, cms.Status as IndStatus, pdms.Status as PDMSStatus, mv.DepartureCountry, mv.PFNo, mv.Status,
    replace(convert(char(11),mv.EmPDepartureDate,113),' ','-') DepartureDate, 
    replace(convert(char(11),mv.PoEArrivalDate,113),' ','-') ArrivalDate, mv.EmbarkationPort, mv.EntryPort
from
    ac
inner join MigrantCase as mc on mc.CaseNo = ac.CaseNo
inner join CaseMember as cm on ac.CaseNo = cm.CaseNo 
inner join CaseMemberFunctionalArea as cfa on cfa.CaseMemberID = cm.CaseMemberID 
                                             and cfa.FunctionalAreaCode = 'HA' 
inner join CaseMemberMedicalService as cms on cfa.CaseMemberID = cms.CaseMemberID 
                                             and cfa.CaseMemberFunctionalAreaID = cms.CaseMemberFunctionalAreaID and cms.MedServiceCode = @peiom
left join CaseMemberMedServiceOtherInfo o with(nolock) on o.CaseMemberMedServiceID=cms.CaseMemberMedicalServiceID 
                                             and o.FieldNo=@panelSiteFieldNo and o.Value<>'' --PanelSite
left join ServiceProvider sp with(nolock) on cms.MedServiceProviderCode=sp.ServiceProviderCode  --Serviceprovider
left join Country ctry on mc.LocationCountry = ctry.Code
left join Country nat on cm.Nationality = nat.Code
left join Country cob on cm.BirthCountry = cob.Code
left join MovementCases mvc on mvc.CaseNo = ac.CaseNo
left join Movement mv on mv.PFNo = mvc.PFNo
left join CaseMemberFunctionalArea as pdms with (nolock) on pdms.CaseMemberID = cm.CaseMemberID and pdms.FunctionalAreaCode = 'PDMS' 
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
where mc.LocationCountry in ('AO', 'BI', 'BW', 'CM', 'CF', 'TD', 'CG', 'CI', 'DJ', 'ER', 'ET', 'GA', 'GH', 'GN', 'KE', 'MG', 'MW', 'MR', 'MZ', 'NA', 'NG', 'NE', 'SN', 'SS', 'SO','TD','TG', 'TZ', 'UG', 'ZA', 'ZM', 'ZW', 'SD', 'CD', 'RW', 'GM') and cfa.ExamDate >= convert(datetime, '2020-01-01')
),

LatestDocStatus
as
(
	select * from 
    (select doc.CaseMemberID, doc.Status, lkp.Description, doc.StatusDate, doc.StartDate, 
	row_number() over (partition by doc.CaseMemberID order by doc.StartDate desc) as StatusRank 
	from  has 
	inner join MedDocumentStatus doc on has.Casememberid =  doc.CaseMemberID
    inner join MedicalLookup lkp on doc.Status = lkp.Code and lkp.LookupGroup='MedicalDocumentStatus')
	l where l.StatusRank = 1
),

has2
as
(
select has.*, l.Description as LatestDocStatus, replace(convert(char(11),l.StatusDate,113),' ','-') LatestDocStatusDate
from has left join LatestDocStatus l on has.CaseMemberID = l.CaseMemberID
),

has_rnk
as
(
select has2.*, hld.isFamilyHold, row_number() over (partition by CaseMemberID order by CFACreatedDate desc) as rnk
from has2
left join (
        select 
            x.CaseNo,
            1 isFamilyHold
        from has x
        where x.MedStatus='HAHLD'
        group by x.CaseNo
    ) hld  on hld.CaseNo = has2.CaseNo
)

select * from has_rnk where rnk = 1
order by
	PrimaryRefNo,
	Seq