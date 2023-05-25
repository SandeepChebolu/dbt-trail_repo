WITH source AS (
	SELECT * FROM {{ source('SAP_ECC','KNVV') }}
),
renamed AS (
	SELECT
		MANDT,
		KUNNR,
		VKORG,
		VTWEG,
		SPART,
		ERNAM,
		ERDAT,
		BEGRU,
		LOEVM,
		VERSG,
		AUFSD,
		KALKS,
		KDGRP,
		BZIRK,
		KONDA,
		PLTYP,
		AWAHR,
		INCO1,
		INCO2,
		LIFSD,
		AUTLF,
		ANTLF,
		KZTLF,
		KZAZU,
		CHSPL,
		LPRIO,
		EIKTO,
		VSBED,
		FAKSD,
		MRNKZ,
		PERFK,
		PERRL,
		KVAKZ,
		KVAWT,
		WAERS,
		KLABC,
		KTGRD,
		ZTERM,
		VWERK,
		VKGRP,
		VKBUR,
		VSORT,
		KVGR1,
		KVGR2,
		KVGR3,
		KVGR4,
		KVGR5,
		BOKRE,
		BOIDT,
		KURST,
		PRFRE,
		PRAT1,
		PRAT2,
		PRAT3,
		PRAT4,
		PRAT5,
		PRAT6,
		PRAT7,
		PRAT8,
		PRAT9,
		PRATA,
		KABSS,
		KKBER,
		CASSD,
		RDOFF,
		AGREL,
		MEGRU,
		UEBTO,
		UNTTO,
		UEBTK,
		PVKSM,
		PODKZ,
		PODTG,
		BLIND,
		CARRIER_NOTIF,
		CVP_XBLCK_V,
		INCOV,
		INCO2_L,
		INCO3_L,
		_BEV1_EMLGPFAND,
		_BEV1_EMLGFORTS,
		FSH_KVGR6,
		FSH_KVGR7,
		FSH_KVGR8,
		FSH_KVGR9,
		FSH_KVGR10,
		FSH_GRREG,
		FSH_RESGY,
		FSH_SC_CID,
		FSH_VAS_DETC,
		FSH_VAS_CG,
		FSH_GRSGY,
		FSH_SS,
		FSH_MSOCDC,
		FSH_MSOPID,
		ZZPAR_ID,
		ZZGGN,
		ZZNGN,
		ZZSGN,
		ZZCPL,
		ZZCPL_EXP_DATE,
		ZZCIL,
		ZZCIL_EXP_DATE,
		ZZKVGR6,
		ZZKVGR7,
		ZZCONS_TIER,
		ZZINST_TIER,
		ZZSERV_TIER,
		META_UPD_DT
	FROM source
)
SELECT * FROM renamed