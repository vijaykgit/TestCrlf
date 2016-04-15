

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.Response;

import com.moremagic.mbroker.restcon.data.merchant.MerchantPropResultSet;
import com.moremagic.mbroker.restcon.data.merchant.MerchantPropUpdateExecutor;
import com.moremagic.mbroker.restcon.model.merchant.request.*;
import com.moremagic.mbroker.restcon.model.merchant.response.*;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.moremagic.mbroker.restcon.annotation.RestconTransactional;
import com.moremagic.mbroker.restcon.authentication.UserSession;
import com.moremagic.mbroker.restcon.bean.user.AccountBean;
import com.moremagic.mbroker.restcon.controller.merchant.MerchantBlockRequest;
import com.moremagic.mbroker.restcon.data.merchant.MerchantStatus;
import com.moremagic.mbroker.restcon.data.merchant.MerchantType;
import com.moremagic.mbroker.restcon.data.user.OtpTypes;
import com.moremagic.mbroker.restcon.error.RestconException;
import com.moremagic.mbroker.restcon.error.RestconLogicError;
import com.moremagic.mbroker.restcon.error.RestconLogicViolation;
import com.moremagic.mbroker.restcon.error.RestconServerError;
import com.moremagic.mbroker.restcon.error.RestconServerFailure;
import com.moremagic.mbroker.restcon.model.customer.response.BillTxnCommissionResponse;
import com.moremagic.mbroker.restcon.model.customer.response.BillTxnFailureResponse;
import com.moremagic.mbroker.restcon.model.customer.response.BillTxnFundoutResponse;
import com.moremagic.mbroker.restcon.model.customer.response.BillTxnPropResponse;
import com.moremagic.mbroker.restcon.model.customer.response.BillTxnResponse;
import com.moremagic.mbroker.restcon.model.merchant.MerchantLimitsSummary;
import com.moremagic.mbroker.restcon.model.merchant.MerchantSummary;
import com.moremagic.mbroker.restcon.model.merchant.MerchantTurnover;
import com.moremagic.mbroker.restcon.model.system.response.CountResponse;
import com.moremagic.mbroker.restcon.service.merchant.MerchantPropService;
import com.moremagic.mbroker.restcon.service.merchant.MerchantService;
import com.moremagic.mbroker.restcon.service.system.AuditTrailService;
import com.moremagic.mbroker.restcon.service.system.SmsService;
import com.moremagic.mbroker.restcon.service.system.SystemPropertyService;
import com.moremagic.mbroker.restcon.service.user.OneTimePasswordService;
import com.moremagic.mbroker.restcon.service.user.TokenService;
import com.moremagic.mbroker.restcon.spring.old.core.services.MessageService;
import com.moremagic.mbroker.restcon.spring.old.core.util.RB;
import com.moremagic.mwallet.core.db.atomics.Merchants;
import com.moremagic.mwallet.core.db.entity.AuditTrail;
import com.moremagic.mwallet.core.db.entity.BankProfile;
import com.moremagic.mwallet.core.db.entity.BillTxn;
import com.moremagic.mwallet.core.db.entity.BillTxnCommission;
import com.moremagic.mwallet.core.db.entity.BillTxnFailure;
import com.moremagic.mwallet.core.db.entity.BillTxnFundout;
import com.moremagic.mwallet.core.db.entity.BillTxnProp;
import com.moremagic.mwallet.core.db.entity.MerchCatTxnLimit;
import com.moremagic.mwallet.core.db.entity.MerchCustomTxnLimit;
import com.moremagic.mwallet.core.db.entity.MerchCustomTxnLimitId;
import com.moremagic.mwallet.core.db.entity.MerchCustomWalletLimit;
import com.moremagic.mwallet.core.db.entity.MerchCustomWalletLimitId;
import com.moremagic.mwallet.core.db.entity.Merchant;
import com.moremagic.mwallet.core.db.entity.MerchantProp;
import com.moremagic.mwallet.core.db.entity.SecurityRole;
import com.moremagic.mwallet.core.db.entity.SystemProperty;
import com.moremagic.mwallet.core.db.entity.enums.AuditEvent;
import com.moremagic.mwallet.core.db.entity.enums.AuditStatus;
import com.moremagic.mwallet.core.db.entity.enums.BillingServiceEnum;
import com.moremagic.mwallet.core.db.entity.enums.UserTypeEnum;
import com.moremagic.mwallet.core.db.entity.enums.WalletType;
import com.moremagic.mwallet.core.db.entity.pendingchange.MerchantPendingChange;
import com.moremagic.mwallet.core.db.pendingchange.PendingChange;
import com.moremagic.mwallet.core.locale.currency.InvalidPriceException;
import com.moremagic.mwallet.core.locale.currency.MWalletPrice;
import com.moremagic.mwallet.core.locale.currency.UnrecognizedCurrencyException;
import com.moremagic.mwallet.management.config.NodeConfig;


@Component
public class MerchantBeanImpl implements MerchantBean {

	@Autowired
	private MerchantPermissionBean merchantPermissionBean;
	
	@Autowired
	private AccountBean accountBean;

	@Autowired
	private MerchantService merchantService;

	@Autowired
	private MerchantPropService merchantPropService;
	
	@Autowired
	private OneTimePasswordService oneTimePasswordService;

	@Autowired
	@Deprecated
	private com.moremagic.mbroker.restcon.spring.old.core.services.merchant.MerchantService oldMerchantService;

	@Autowired
	@Deprecated
	private MessageService oldMessageService;

	@Autowired
	private SmsService smsService;
	@Autowired
	@Deprecated
	private RB rb;

	@Autowired
	private AuditTrailService auditTrailService;
	
	@Autowired
	private TokenService tokenService;
	

	@Autowired
	private SystemPropertyService sysPropService;
	
	private static final List<String> APPROVALABLE_TYPE_LIST = Arrays.asList(new String[] { MerchantType.TYPE_MERCHANT, MerchantType.TYPE_MASTER_ADMIN});


	private static final Logger logger = Logger.getLogger(MerchantBeanImpl.class);

	@Override
	public List<MerchantSummary> getMerchants(String userId, String search, String searchname, String types,  String status,
			String nickNumber, String sortField, String sortDir, Integer firstResult, Integer maxResults)
			throws RestconException {
        logger.debug("getMerchants in merchantBean. ");
		Merchant merchant = getMerchant(userId);
		String rootId = determineRootId(merchant);

		if (firstResult == null) {
			firstResult = 0;
		}

		if (maxResults == null) {
			maxResults = 50;
		}

		List<String> filterTypes = parseTypes(types);
        logger.debug("finish getMerchant in merchantBean");
		List<MerchantSummary> merchants = merchantService.getMerchants(rootId, search, searchname, filterTypes, status, nickNumber, sortField, sortDir,
				firstResult, maxResults, merchant.getTenantId());
		
		for (MerchantSummary merch : merchants) {
			merch.setType(MerchantType.convertTypeForDisplay(merch.getType(), merch.getFlags() == null ? null : merch.getFlags().longValue() ));
			merch.setFlags(new BigDecimal(0)); //obscurity.  Really, service layer and restcon layer should use different objects.  //TODO
		}

		return merchants;
	}

	@Override
	public MerchantListCountResponse countMerchants(String userId, String search, String searchName, String types, 
			String status, String nickNumber) throws RestconException {

		Merchant merchant = getMerchant(userId);

		String rootId = determineRootId(merchant);

		List<String> filterTypes = parseTypes(types);

		int result = merchantService.getMerchantsCount(rootId, search, searchName, filterTypes, status, merchant.getTenantId(), nickNumber);

		return new MerchantListCountResponse(result);
	}

	private String determineRootId(Merchant merchant) throws RestconLogicViolation {

		String rootId = null;
		switch (merchant.getType()) {
		case Merchant.TYPE_ADMIN:
			//Admins can see the whole system.
			rootId = null;
			break;
		case Merchant.TYPE_MASTER_MANAGER:
		case Merchant.TYPE_MANAGER:
		case Merchant.TYPE_EMPLOYEE:
			//MUI users can see their parent and their parent's children.
			rootId = merchant.getParentId();
			break;
		default:
			//Any other type should not be doing this at all.
			throw new RestconLogicViolation(RestconLogicError.MERCHANT_ACTOR_INVALID);
		}
		
		return rootId;
	}

	@Override
	public List<MerchantSummary> getListOfSuperAgentForApproval(String searchname, String searchid, String types, 
			Integer firstResult, Integer maxResults, String loggedInUser) throws RestconException {

		Merchant loggedInUserMerchant = merchantService.find(loggedInUser, UserTypeEnum.MERCHANT_COMMON_ID);

		List<MerchantSummary> merchants =null;
		if(types==null||types.isEmpty()){
				merchants=merchantService.getMerchants(null, searchid, searchname, APPROVALABLE_TYPE_LIST, 
				Merchant.STATUS_UNACTIVATED,  null, firstResult, maxResults, loggedInUserMerchant.getTenantId());
		}else{
			List<String> filterTypes = parseTypes(types);
			List<String> approvalTypes= new LinkedList<String>();
			for (String parseType: filterTypes){
				for(String type:APPROVALABLE_TYPE_LIST){
					if(type.equals(parseType)){
						approvalTypes.add(type);
					}
				}
			}
			if(approvalTypes.size()==0){
				return new LinkedList<MerchantSummary>();
			}
			merchants=merchantService.getMerchants(null, searchid, searchname, approvalTypes, 
					Merchant.STATUS_UNACTIVATED,  null, firstResult, maxResults,loggedInUserMerchant.getTenantId());
		}
		
		for (MerchantSummary merchant : merchants) 
		{
			//Merchant createdMerchant = merchantService.find(merchant.getMasterManagerType(), UserTypeEnum.MERCHANT_SUR_ID);
			Merchant createdMerchant = null;
			String createdId = merchantPropService.findValue(merchant.getId(),MerchantProp.MERCHANT_CREATED_BY);
			if(createdId!=null){
				createdMerchant=merchantService.find(createdId,UserTypeEnum.MERCHANT_SUR_ID);
				logger.debug("createdMerchant: "+createdMerchant.getMerchantId());
			}
			if (createdMerchant != null)
			{
				merchant.setCreatedBy(createdMerchant.getMerchantId());
				merchant.setCanBeApproved(!loggedInUserMerchant.getId().equals(createdMerchant.getId()));	
			}
			else
			{
				merchant.setCreatedBy("-");
				merchant.setCanBeApproved(true);
			}
		}
		return merchants;
	}
	
	@Override
	@RestconTransactional
	public String completeMerchantRegistration(String merchId, MerchantApprovalRequest request, String userId, String ipAddress,
			UserSession userSession) throws RestconException {

		Merchant actor = getMerchant(userId);
		Merchant merchant = getMerchantTenantSafe(merchId,actor.getTenantId());
		
		if (Merchants.hasAncestorFullSystemBlock(merchant)) {
			throw new RestconLogicViolation(
					RestconLogicError.MERCHANT_ANCESTOR_FULL_BLOCKED);
		}

		Merchant createdMerchant = null;
		String createdId = merchantPropService.findValue(merchant.getId(),
				MerchantProp.MERCHANT_CREATED_BY);
		if (createdId != null) {
			createdMerchant = merchantService.find(createdId,
					UserTypeEnum.MERCHANT_SUR_ID);
			logger.debug("createdMerchant: " + createdMerchant.getMerchantId());
			if (createdMerchant.getId().equals(actor.getId())) {
				logger.error("merchant can't be approved by creator");
				throw new RestconLogicViolation(
						RestconLogicError.MERCHANT_CANNOT_EDIT);
			}
		}

		AuditTrail auditTrail = auditTrailService.logEvent(
				AuditEvent.CONFIRM_MERCHANT_REGISTRATION, null,
				AuditStatus.SUCCESS, userSession.getDeviceType(),
				actor.getId(), actor.getIdType(), merchant.getId(),
				merchant.getIdType(), ipAddress, null, null, null, null,
				actor.getTenantId());

		if (!(merchant.getType().equals(Merchant.TYPE_SUPER_AGENT) || merchant
				.getType().equals(Merchant.TYPE_MASTER_MANAGER))
				|| !merchant.getStatus().equals(Merchant.STATUS_UNACTIVATED)) {
			throw new RestconLogicViolation(
					RestconLogicError.MERCHANT_CANNOT_EDIT);
		}

		String newStatus = MerchantStatus.convertToDatabaseStatus(request
				.getNewStatus());
		if (newStatus == null
				|| !(newStatus.equals(Merchant.STATUS_REJECTED) || newStatus
						.equals(Merchant.STATUS_APPROVED))) {
			throw new RestconServerFailure(RestconServerError.REQUEST_PARSING);
		}

		Map<String, String> propToChange = new HashMap<String, String>(2);
		if (newStatus.equals(Merchant.STATUS_REJECTED)) {
			if (request.getChangeReason() != null) {
				propToChange.put(MerchantProp.APPROVAL_REJECTION_REASON,
						request.getChangeReason());
			}
			propToChange.put(MerchantProp.APPROVAL_REJECTION_USER,
					actor.getId());
			merchant.setStatus(newStatus);
		} else if (newStatus.equals(Merchant.STATUS_APPROVED)) {
			if (request.getChangeReason() != null) {
				propToChange.put(MerchantProp.MERCHANT_ACTIVATED_REASON,
						request.getChangeReason());
			}
			propToChange.put(MerchantProp.MERCHANT_ACTIVATED_BY,
					actor.getId());
			if (merchant.getType().equals(Merchant.TYPE_SUPER_AGENT)) {
				merchant.setStatus(Merchant.STATUS_APPROVED);
			} else {
				merchant.setStatus(newStatus);
			}
		}

		try {
			merchantPropService.updateOrCreatePropInSet(propToChange,
					merchant.getId(), actor.getTenantId(), auditTrail);
		} catch (Exception e) {
			logger.error(
					"Error setting the activate/suspend reason property on the merchant prop",
					e);
			throw new RestconServerFailure(
					RestconServerError.DATABASE_LOG_STATE_CHANGE);
		}

		// merchant.setStatus(newStatus);

		merchant = merchantService.update(merchant, auditTrail);

		if (newStatus.equals(Merchant.STATUS_REJECTED)) {
			return "success.rejection";
		}

		String displayType = rb.getByLocale("label_type_masterManager", NodeConfig.getDefaultLocale("messages"));
		if (merchant.getType().equals(Merchant.TYPE_SUPER_AGENT)) {
			displayType = rb.getByLocale("label_type_superAgent", NodeConfig.getDefaultLocale("messages"));
		}
		oldMessageService.sendEmail(
				actor.getEmail(),
				rb.getByLocale("merchant_approved_subject", NodeConfig.getDefaultLocale("messages"), displayType),
				rb.getByLocale("merchant_approved_by_admin_body", NodeConfig.getDefaultLocale("messages"), displayType,
						merchant.getMerchantId()), actor);

		if (merchant.getType().equals(Merchant.TYPE_MASTER_MANAGER)) {
	
			accountBean.registrationPassword(merchant, displayType, auditTrail);
			
		}else{
			oldMessageService.sendEmail(
					merchant.getEmail(),
					rb.getByLocale("merchant_approved_subject", NodeConfig.getDefaultLocale("messages"), displayType),
					rb.getByLocale("merchant_approved_body", NodeConfig.getDefaultLocale("messages"), displayType,
							merchant.getMerchantId(),merchant.getName()), merchant);
		}
		return "success.approval";
	}

	
	@Override
	@RestconTransactional
	public void blockMerchant(String userId, String merchantId, MerchantBlockRequest request,
			String ipAddress, UserSession userSession) throws RestconException {
		
		// Verify calling user exists
		Merchant actor = getMerchant(userId);
		Merchant merchant = getMerchantTenantSafe(merchantId,actor.getTenantId());

		if(!checkBlockPermission(actor, merchant, request)){
			throw new RestconLogicViolation(
					RestconLogicError.MERCHANT_BLOCK_INVALID);
		}
		// can't block registered merchant or approved merchant
		if (!merchant.getStatus().equals(Merchant.STATUS_ACTIVE)) {
			throw new RestconLogicViolation(
					RestconLogicError.MERCHANT_NOT_ACTIVE);
		}

		boolean isPartial = request.isPartial();
		boolean hasFullBlock = Merchants.hasFlag(merchant,
				Merchant.FLAG_BLOCK_SYSTEM_FULL);
		boolean hasAnyBlock = Merchants.hasFlag(merchant,
				Merchant.FLAG_BLOCK_SYSTEM_PARTIAL) || hasFullBlock;

		if ((isPartial && hasAnyBlock) || (!isPartial && hasFullBlock)) {
			throw new RestconLogicViolation(
					RestconLogicError.MERCHANT_BLOCK_ALREADY_BLOCKED);
		} else if (request.isPartial()) {
			AuditTrail auditTrail = auditTrailService.logEvent(
					AuditEvent.BLOCK_SYSTEM_PARTIAL, null, AuditStatus.SUCCESS,
					userSession.getDeviceType(), actor.getId(),
					actor.getIdType(), merchant.getId(), merchant.getIdType(),
					ipAddress, null, null, null, null, actor.getTenantId());
			merchantService.blockMerchant(merchant, request.getBlockReason(),
					userId, auditTrail, true);
			sendSelfBlockEmail(merchant);
		} else {
			if (merchant.getType().equals(Merchant.TYPE_MASTER_MANAGER)||
					(merchant.getType().equals(Merchant.TYPE_EMPLOYEE)
					/*&& Merchants.hasFlag(merchant, Merchant.FLAG_VIRTUAL_EMPLOYEE)*/)||
					merchant.getType().equals(Merchant.TYPE_MANAGER)){
				// for full block, block merchant only for master admin
				// Remove pending block if upgrading to full
				fullBlockMerchantSingle(actor, merchant, request, ipAddress, userSession);
				sendSelfBlockEmail(merchant);
			} else if (merchant.getType().equals(Merchant.TYPE_SUPER_AGENT)) {
				// for full block block hierarchy
				Set<String> hierarchyIds = merchantService
						.getVisibleHierarchySurIds(merchant.getMerchantId());
				for (String childId : hierarchyIds) {
					Merchant child = merchantService.find(childId,
							UserTypeEnum.MERCHANT_SUR_ID);
					if (child != null
							&& child.getStatus().equals(Merchant.STATUS_ACTIVE)) {
						if (Merchants.hasBlock(child,
								Merchant.FLAG_BLOCK_SYSTEM_FULL)) {
							continue;
						}
						fullBlockMerchantSingle(actor, child, request, ipAddress,
								userSession);
						if(child.getId().equals(merchant.getId())){
							sendHierarchyBlockEmail(merchant, merchant, true);
						}else{
							sendHierarchyBlockEmail(child, merchant,false);
						}
					}
				}
			}
		}
	}
	
	private boolean checkBlockPermission(Merchant actor, Merchant merchant, 
			MerchantBlockRequest request){
		if(actor.getType().equals(Merchant.TYPE_ADMIN)){
			if(merchant.getType().equals(Merchant.TYPE_SUPER_AGENT)||
					merchant.getType().equals(Merchant.TYPE_MASTER_MANAGER)||
					merchant.getType().equals(Merchant.TYPE_MANAGER)||
					merchant.getType().equals(Merchant.TYPE_EMPLOYEE)){
				return true;
			}
		}
		if(actor.getType().equals(Merchant.TYPE_MASTER_MANAGER)){
				 if(merchant.getType().equals(Merchant.TYPE_EMPLOYEE)
				/*&& Merchants.hasFlag(merchant, Merchant.FLAG_VIRTUAL_EMPLOYEE)*/){
					 if(!request.isPartial()){
						 return true;
					 }
				 }
		}
		return false;
	}
	
	private boolean checkUnblockPermission(Merchant actor, Merchant merchant){
		if(actor.getType().equals(Merchant.TYPE_ADMIN)){
			if(merchant.getType().equals(Merchant.TYPE_SUPER_AGENT)||
					merchant.getType().equals(Merchant.TYPE_MASTER_MANAGER)||
					merchant.getType().equals(Merchant.TYPE_MANAGER)||
					merchant.getType().equals(Merchant.TYPE_EMPLOYEE)){
				return true;
			}
		}
		if(actor.getType().equals(Merchant.TYPE_MASTER_MANAGER)){
				 if(merchant.getType().equals(Merchant.TYPE_EMPLOYEE)
				&& Merchants.hasFlag(merchant, Merchant.FLAG_VIRTUAL_EMPLOYEE)){
					return true;
				 }
		}
		return false;
	}
	
	
	private boolean checkApproveUnblockPermission(Merchant actor, Merchant merchant){
		if(actor.getType().equals(Merchant.TYPE_ADMIN)){
			if(merchant.getType().equals(Merchant.TYPE_SUPER_AGENT)||
					merchant.getType().equals(Merchant.TYPE_MASTER_MANAGER)||
					merchant.getType().equals(Merchant.TYPE_MANAGER)||
					merchant.getType().equals(Merchant.TYPE_EMPLOYEE)){
				return true;
			}
		}
		return false;
	}
	

	private void sendSelfBlockEmail(Merchant merchant){
		String displayType = rb.getByLocale("label_type_masterManager", NodeConfig.getDefaultLocale("messages"));
		if(merchant.getType().equals(Merchant.TYPE_SUPER_AGENT)){
			displayType=rb.getByLocale("label_type_superAgent", NodeConfig.getDefaultLocale("messages"));
		}else if(merchant.getType().equals(Merchant.TYPE_EMPLOYEE)
				&& Merchants.hasFlag(merchant,Merchant.FLAG_VIRTUAL_EMPLOYEE)){
			displayType=rb.getByLocale("label_type_virtualEmployee", NodeConfig.getDefaultLocale("messages"));
		}
		String subject = rb.getByLocale("merchant_block_self_subject", NodeConfig.getDefaultLocale("messages"), displayType);
		String body = rb.getByLocale("merchant_block_self_body", NodeConfig.getDefaultLocale("messages"), displayType, merchant.getMerchantId());
		oldMessageService.sendEmail(merchant.getEmail(), subject, body, merchant);
	}
	
	private void sendHierarchyBlockEmail(Merchant merchant, Merchant parent, boolean isRoot){
		String displayType = rb.getByLocale("label_type_masterManager", NodeConfig.getDefaultLocale("messages"));
		if(merchant.getType().equals(Merchant.TYPE_SUPER_AGENT)){
			displayType=rb.getByLocale("label_type_superAgent", NodeConfig.getDefaultLocale("messages"));
		}else if(merchant.getType().equals(Merchant.TYPE_EMPLOYEE)
				&& Merchants.hasFlag(merchant,Merchant.FLAG_VIRTUAL_EMPLOYEE)){
			displayType=rb.getByLocale("label_type_virtualEmployee", NodeConfig.getDefaultLocale("messages"));
		}
		String subject = rb.getByLocale("merchant_block_child_subject", NodeConfig.getDefaultLocale("messages"), displayType);
		String body = rb.getByLocale("merchant_block_child_body", NodeConfig.getDefaultLocale("messages"), parent.getMerchantId());
		if(isRoot){
			subject =  rb.getByLocale("merchant_block_hierarchy_subject", NodeConfig.getDefaultLocale("messages"), merchant.getMerchantId());
			body=  rb.getByLocale("merchant_block_hierarchy_body", NodeConfig.getDefaultLocale("messages"), merchant.getName());
		}
		oldMessageService.sendEmail(merchant.getEmail(), subject, body, merchant);
	}
	
	private void fullBlockMerchantSingle(Merchant actor, Merchant merchant, MerchantBlockRequest request, String ipAddress,
 UserSession userSession) {
		if (Merchants.hasBlock(merchant, Merchant.FLAG_BLOCK_SYSTEM_PARTIAL)) {
			AuditTrail auditTrail = auditTrailService.logEvent(
					AuditEvent.UNBLOCK_SYSTEM_PARTIAL, null,
					AuditStatus.SUCCESS, userSession.getDeviceType(),
					actor.getId(), actor.getIdType(), merchant.getId(),
					merchant.getIdType(), ipAddress, null, null, null, null,
					actor.getTenantId());
			merchantService.unblockMerchant(merchant, auditTrail, true);
		}
		AuditTrail auditTrail = auditTrailService.logEvent(
				AuditEvent.BLOCK_SYSTEM_FULL, null, AuditStatus.SUCCESS,
				userSession.getDeviceType(), actor.getId(), actor.getIdType(),
				merchant.getId(), merchant.getIdType(), ipAddress, null, null,
				null, null, actor.getTenantId());
		merchantService.blockMerchant(merchant, request.getBlockReason(),
				actor.getId(), auditTrail, false);
	}

	@Override
	@RestconTransactional
	public void unblockMerchant(String userId, String merchantId, String ipAddress,UserSession userSession)
			throws RestconException {
		
		// Verify calling user exists
		Merchant actor = getMerchant(userId);
		Merchant merchant = getMerchantTenantSafe(merchantId,actor.getTenantId());

		if(!checkUnblockPermission(actor, merchant)){
			throw new RestconLogicViolation(
					RestconLogicError.MERCHANT_UNBLOCK_INVALID);
		}
		
		if(Merchants.hasAncestorFullSystemBlock(merchant)){
			throw new RestconLogicViolation(
					RestconLogicError.MERCHANT_ANCESTOR_FULL_BLOCKED);
		}
		// checkMerchantBlockingPermissions(userId, merchant.getType());

		if (Merchants.hasFlag(merchant, Merchant.FLAG_BLOCK_SYSTEM_PARTIAL)) {
			AuditTrail auditTrail = auditTrailService.logEvent(
					AuditEvent.UNBLOCK_SYSTEM_PARTIAL, null,
					AuditStatus.SUCCESS, userSession.getDeviceType(),
					actor.getId(), actor.getIdType(), merchant.getId(),
					merchant.getIdType(), ipAddress, null, null, null, null,
					actor.getTenantId());
			merchantService.unblockMerchant(merchant, auditTrail, true);
			sendUnblockEmail(merchant,true);
		} else if (Merchants.hasFlag(merchant, Merchant.FLAG_BLOCK_SYSTEM_FULL)) {
			/*if (merchant.getType().equals(Merchant.TYPE_MASTER_MANAGER)) {
				fullUnblockMerchantSingle(actor, merchant, ipAddress,
						userSession);
				sendUnblockEmail(merchant,true);
			} else if (merchant.getType().equals(Merchant.TYPE_SUPER_AGENT)) {
				// for full block block hierarchy
				Set<String> hierarchyIds = merchantService
						.getVisibleHierarchySurIds(merchant.getMerchantId());
				for (String childId : hierarchyIds) {
					Merchant child = merchantService.find(childId,
							UserTypeEnum.MERCHANT_SUR_ID);
					if (child != null
							&& Merchants.hasBlock(child,
									Merchant.FLAG_BLOCK_SYSTEM_FULL)) {
						fullUnblockMerchantSingle(actor, child, ipAddress,
								userSession);
					}
					if(merchant.getId().equals(child.getId())){
						sendUnblockEmail(merchant,false);
					}else{
						sendUnblockEmail(child,true);
					}
				}
			}*/
			if (merchant.getType().equals(Merchant.TYPE_EMPLOYEE) /*&&
					Merchants.hasFlag(merchant,Merchant.FLAG_VIRTUAL_EMPLOYEE)*/) {
				fullUnblockMerchantSingle(actor, merchant, ipAddress,
						userSession);
				sendUnblockEmail(merchant,true);
			}else if(merchant.getType().equals(Merchant.TYPE_MASTER_MANAGER)||
                   merchant.getType().equals(Merchant.TYPE_MANAGER)) {
				
				/*MerchantPendingChange pendingUnblock = merchantService
						.findMerchantPendingChange(merchant,
								PendingChange.EVENT_UNBLOCK_SYSTEM_FULL);
				if (pendingUnblock != null) {
					logger.error("already has pending unblock");
					throw new RestconLogicViolation(
							RestconLogicError.MERCHANT_UNBLOCK_INVALID);
				}
				addPendingUnblock(actor, merchant, ipAddress, userSession);*/
				
				fullUnblockMerchantSingle(actor, merchant, ipAddress,
						userSession);
				sendUnblockEmail(merchant,true);
			}else if (merchant.getType().equals(Merchant.TYPE_SUPER_AGENT)) {
				// for full block block hierarchy
				Set<String> hierarchyIds = merchantService
						.getVisibleHierarchySurIds(merchant.getMerchantId());
				for (String childId : hierarchyIds) {
					Merchant child = merchantService.find(childId,
							UserTypeEnum.MERCHANT_SUR_ID);
					if (child != null
							&& Merchants.hasBlock(child,
									Merchant.FLAG_BLOCK_SYSTEM_FULL)) {
						fullUnblockMerchantSingle(actor, child, ipAddress,
								userSession);
					}
					if(merchant.getId().equals(child.getId())){
						sendUnblockEmail(merchant,false);
					}else{
						sendUnblockEmail(child,true);
					}
				}
			}
		} else {
			throw new RestconLogicViolation(
					RestconLogicError.MERCHANT_UNBLOCK_NOT_BLOCKED);
		}
	}
	
	@Override
	@RestconTransactional
	public void approveUnblockMerchant(String userId, String merchantId, MerchantChangeApprovalRequest request, String ipAddress,UserSession userSession)
 throws RestconException {
		// Verify calling user exists
		Merchant actor = getMerchant(userId);
		Merchant merchant = getMerchantTenantSafe(merchantId,actor.getTenantId());

		if(!checkApproveUnblockPermission(actor, merchant)){
			throw new RestconLogicViolation(
					RestconLogicError.MERCHANT_UNBLOCK_INVALID);
		}
		
		if(Merchants.hasAncestorFullSystemBlock(merchant)){
			throw new RestconLogicViolation(
					RestconLogicError.MERCHANT_ANCESTOR_FULL_BLOCKED);
		}
		MerchantPendingChange pendingUnblock = merchantService
				.findMerchantPendingChange(merchant,
						PendingChange.EVENT_UNBLOCK_SYSTEM_FULL);
		if(pendingUnblock == null){
			logger.error("no pending unblock find");
			throw new RestconLogicViolation(
					RestconLogicError.MERCHANT_PENDING_UNBLOCK_NOT_FOUND);
		}else{
			String preUserId=pendingUnblock.getPendingChangeCommon().getUserId();
			logger.debug("preUnblockId: "+preUserId);
			if(preUserId.equals(actor.getId())){
				logger.error("approve unblock, unblock can't be same user");
				throw new RestconLogicViolation(
						RestconLogicError.UNBLOCK_APPORVED_BY_SAME_USER);
			}
		}
		if(!request.getIsApproved()){
			logger.debug("rejected unblock merchant, delete pending changes");
			merchantService.deleteMerchantPendingChange(pendingUnblock);
			return;
		}
		if (merchant.getType().equals(Merchant.TYPE_MASTER_MANAGER)||
				merchant.getType().equals(Merchant.TYPE_MANAGER)) {
			fullUnblockMerchantSingle(actor, merchant, ipAddress,
					userSession);
			sendUnblockEmail(merchant,true);
		} else if (merchant.getType().equals(Merchant.TYPE_SUPER_AGENT)) {
			// for full block block hierarchy
			Set<String> hierarchyIds = merchantService
					.getVisibleHierarchySurIds(merchant.getMerchantId());
			for (String childId : hierarchyIds) {
				Merchant child = merchantService.find(childId,
						UserTypeEnum.MERCHANT_SUR_ID);
				if (child != null
						&& Merchants.hasBlock(child,
								Merchant.FLAG_BLOCK_SYSTEM_FULL)) {
					fullUnblockMerchantSingle(actor, child, ipAddress,
							userSession);
				}
				if(merchant.getId().equals(child.getId())){
					sendUnblockEmail(merchant,false);
				}else{
					sendUnblockEmail(child,true);
				}
			}
		}
		merchantService.deleteMerchantPendingChange(pendingUnblock);
	}
	
	private void addPendingUnblock(Merchant actor, Merchant merchant,
			String ipAddress, UserSession userSession) {
		logger.debug("add pending unblock. ");
		AuditTrail auditTrail = auditTrailService.logEvent(
				AuditEvent.UNBLOCK_SYSTEM_FULL, null, AuditStatus.SUCCESS,
				userSession.getDeviceType(), actor.getId(), actor.getIdType(),
				merchant.getId(), merchant.getIdType(), ipAddress, null, null,
				null, null, actor.getTenantId());
		merchantService.addPendingFullUnblock(actor, merchant, auditTrail);
	}
		
	
	private void sendUnblockEmail(Merchant merchant, boolean isSelf){
		String displayType = rb.getByLocale("label_type_masterManager", NodeConfig.getDefaultLocale("messages"));
		if(merchant.getType().equals(Merchant.TYPE_SUPER_AGENT)){
			displayType=rb.getByLocale("label_type_superAgent", NodeConfig.getDefaultLocale("messages"));
		}else if(merchant.getType().equals(Merchant.TYPE_EMPLOYEE)
				&& Merchants.hasFlag(merchant,Merchant.FLAG_VIRTUAL_EMPLOYEE)){
			displayType=rb.getByLocale("label_type_virtualEmployee", NodeConfig.getDefaultLocale("messages"));
		}
		String subject = rb.getByLocale("merchant_unblock_self_subject", NodeConfig.getDefaultLocale("messages"), displayType);
		String body = rb.getByLocale("merchant_unblock_self_body", NodeConfig.getDefaultLocale("messages"), displayType, merchant.getMerchantId());
		if(!isSelf){
			subject =  rb.getByLocale("merchant_unblock_hierarchy_subject", NodeConfig.getDefaultLocale("messages"), merchant.getMerchantId());
			body=  rb.getByLocale("merchant_unblock_hierarchy_body", NodeConfig.getDefaultLocale("messages"), merchant.getName());
		}
		oldMessageService.sendEmail(merchant.getEmail(), subject, body, merchant);
	}
	
	private void fullUnblockMerchantSingle(Merchant actor, Merchant merchant,
			String ipAddress, UserSession userSession) {
		AuditTrail auditTrail = auditTrailService.logEvent(
				AuditEvent.UNBLOCK_SYSTEM_FULL, null, AuditStatus.SUCCESS,
				userSession.getDeviceType(), actor.getId(), actor.getIdType(),
				merchant.getId(), merchant.getIdType(), ipAddress, null, null,
				null, null, actor.getTenantId());
		merchantService.unblockMerchant(merchant, auditTrail, false);
	}
	
	private List<String> parseTypes(String types) {
		List<String> typesList = new LinkedList<String>();
		if (types != null && !types.isEmpty()) {
			String[] ts = types.split(",", 0);
			for (String t : ts) {
				//String ft = MerchantType.convertTypeForDatabase(t);
				//if (ft != null) {
					typesList.add(t);
				//}
			}
		}

		return typesList;
	}

	@Override
	@RestconTransactional
	public MerchantCareTokenResponse issueMerchantCareToken(String userId, String requestIp) throws RestconException {
		//Verify calling user exists
		getMerchant(userId);
		
		String token = oneTimePasswordService.createTokenWithoutSending(userId, OtpTypes.MERCHANT_CARE_TOKEN.getServerName(), requestIp);
		
		return new MerchantCareTokenResponse(token);
	}
	
	@Override
	@RestconTransactional
	public MerchantTurnoverResponse merchantTurnover(String userId, Integer days) throws RestconException {

		Merchant merchant = getMerchant(userId);
		
		if(days == null) {
			days = 1;
		}
		
		Set<String> permissions = merchantPermissionBean.getPermissions(userId);
		
		List<MerchantTurnover> merchantTurnover = null;
		

		if(permissions.contains(SecurityRole.PERMISSION_MUI_VIEW_TRANSACTION)) {
			Set<String> hierarchyUserAndSurIds = merchantService.getHierarchyUserAndSurIds(merchant.getRootId());
			merchantTurnover = merchantService.getMerchantTurnover(hierarchyUserAndSurIds, days);
			merchantService.getHierarchyUserAndSurIds(merchant.getRootId());
		}
		
		Set<String> branchUserAndSurIds = merchantService.getHierarchyUserAndSurIds(merchant.getParentId());
		
		List<MerchantTurnover> branchTurnover = merchantService.getBranchTurnover(branchUserAndSurIds, days);
		List<MerchantTurnover> userTurnover = merchantService.getUserTurnover(merchant, days);
		
		MerchantTurnoverResponse response = new MerchantTurnoverResponse();
		
		response.setMerchantTurnover(merchantTurnover);
		response.setBranchTurnover(branchTurnover);
		response.setUserTurnover(userTurnover);
		
		return response;
	}
	
	public Merchant getMerchant(String userId) throws RestconException {
		Merchant merchant = merchantService.find(userId, UserTypeEnum.MERCHANT_COMMON_ID);

		if (merchant == null) {
			throw new RestconLogicViolation(RestconLogicError.MERCHANT_DOES_NOT_EXIST);
		}

		return merchant;
	}
	
	private Merchant getMerchantTenantSafe(String userId, Long tenantId) throws RestconLogicViolation {
		Merchant merchant = merchantService.find(userId,UserTypeEnum.MERCHANT_COMMON_ID);

		if (merchant == null || !merchant.getTenantId().equals(tenantId) ) {
			throw new RestconLogicViolation(RestconLogicError.MERCHANT_DOES_NOT_EXIST);
		}

		return merchant;
	}

	private void checkMerchantBlockingPermissions(String userId, String type) throws RestconException {
		Set<String> permissions = merchantPermissionBean.getPermissions(userId);

		if (type != null) {
			if (type.equals(Merchant.TYPE_AGENT)) {
				if (!permissions.contains(SecurityRole.PERMISSION_MUI_VIEW_BRANCH)) {
					throw new RestconLogicViolation(RestconLogicError.GENERIC_PERMISSION);
				}
			} else if (type.equals(Merchant.TYPE_MASTER_MANAGER)) {
				throw new RestconLogicViolation(RestconLogicError.MERCHANT_BLOCK_MASTER_ADMIN_NOT_ALLOWED);
			} else if (!permissions.contains(SecurityRole.PERMISSION_MUI_VIEW_MERCHANTS)) {
				throw new RestconLogicViolation(RestconLogicError.GENERIC_PERMISSION);
			}
		} else if (!permissions.contains(SecurityRole.PERMISSION_MUI_VIEW_BRANCH) && !permissions.contains(SecurityRole.PERMISSION_MUI_VIEW_MERCHANTS)) {
			throw new RestconLogicViolation(RestconLogicError.GENERIC_PERMISSION);
		}
	}

	
	@Override
	@RestconTransactional
	public void deleteMerchant(String merchId, MerchantStatusUpdateRequest request, String userId, String ipAddress, UserSession userSession)
			throws RestconException {
		
		Merchant actor = getMerchant(userId);
		Merchant m = getMerchantTenantSafe(merchId,actor.getTenantId());
		
		if(!checkDeletePermission(actor,m)){
			throw new RestconLogicViolation(RestconLogicError.MERCHANT_CANNOT_DELETE);
		}
		

		AuditTrail auditTrail = auditTrailService.logEvent(AuditEvent.DELETE_MERCHANT, request.getChangeReason(), AuditStatus.SUCCESS, userSession.getDeviceType(),
				actor.getId(), actor.getIdType(), m.getId(), m.getIdType(), ipAddress, null, null, null, null,actor.getTenantId());

		List<MerchantSummary> merchants = merchantService.getMerchants(m.getId(), null, null, null, null, null, 0, Integer.MAX_VALUE,actor.getTenantId());
		List<MerchantSummary> registeredMerchants = merchantService.getMerchants(m.getId(), null, null, null, Merchant.STATUS_UNACTIVATED, null, 0, Integer.MAX_VALUE,actor.getTenantId());
        if(registeredMerchants!=null && !registeredMerchants.isEmpty()){
        	merchants.addAll(registeredMerchants);
        }
		// Merchant must have a zero balance
		if (!m.getBalance().equals(BigDecimal.ZERO)) {
			throw new RestconLogicViolation(RestconLogicError.MERCHANT_DELETE_CANNOT_HAVE_BALANCE);
		}

		// All child merchants must have a zero balance
		for (MerchantSummary merch : merchants) {

			if (!merch.getBalance().equals(BigDecimal.ZERO)) {
				throw new RestconLogicViolation(RestconLogicError.MERCHANT_DELETE_CANNOT_HAVE_CHILDREN_BALANCE, merch.getUserId());
			}
		}

		// All is well.
		for (MerchantSummary merch : merchants) {
			// Getting Children
			Merchant tempMerchant = getMerchant(merch.getUserId());
			//Checking if the hierarchy doesnt have already deleted Merchants. 
			if(tempMerchant.getStatus().equals(Merchant.STATUS_CLOSED)){
				logger.debug("Merchant is already deleted "+tempMerchant.getMerchantId());
				continue;
			}

			// Suspending Children.
			tempMerchant.setStatus(Merchant.STATUS_DELETED);
			merchantService.update(tempMerchant, auditTrail);

			// Reson for Children
			Map<String, String> propToChange = new HashMap<String, String>(2);
			propToChange.put(MerchantProp.MERCHANT_CLOSED_REASON, m.getMerchantId() + " has been deleted, Child Deleted.");
			propToChange.put(MerchantProp.MERCHANT_CLOSED_BY, userId);

			try {
				merchantPropService.updateOrCreatePropInSet(propToChange, tempMerchant.getId(), tempMerchant.getTenantId(), auditTrail);
			} catch (Exception e) {
				logger.error("Error setting the deleted/closed reason property on the merchant prop", e);
				throw new RestconServerFailure(RestconServerError.DATABASE_LOG_STATE_CHANGE);
			}
			sendDeleteMerchantEmail(tempMerchant);

		}

		m.setStatus(Merchant.STATUS_DELETED);
		merchantService.update(m, auditTrail);

		Map<String, String> propToChange = new HashMap<String, String>(1);


		propToChange.put(MerchantProp.MERCHANT_CLOSED_REASON, request.getChangeReason());
		propToChange.put(MerchantProp.MERCHANT_CLOSED_BY, userId);
		try {
			merchantPropService.updateOrCreatePropInSet(propToChange, m.getId(), m.getTenantId(), auditTrail);
		} catch (Exception e) {
			logger.error("Error setting the deleted/closed reason property on the merchant prop", e);
			throw new RestconServerFailure(RestconServerError.DATABASE_LOG_STATE_CHANGE);
		}

		sendDeleteMerchantEmail(m);
	}
	
	private boolean checkDeletePermission(Merchant actor, Merchant merchant){
		
		boolean result = false;
		
		switch (actor.getType()) {
		case Merchant.TYPE_ADMIN:
			result = true;
			break;
			
		case Merchant.TYPE_MASTER_MANAGER:
		case Merchant.TYPE_MANAGER:
			if ( merchant.getType().equals(Merchant.TYPE_MANAGER) ||
					merchant.getType().equals(Merchant.TYPE_EMPLOYEE) ) {result = true;}
			break;
		default:
			break;
		}
		
		return result;
	}

	private void sendDeleteMerchantEmail(Merchant merchant){
		String displayType = rb.getByLocale("label_type_masterManager", NodeConfig.getDefaultLocale("messages"));
		if(merchant.getType().equals(Merchant.TYPE_SUPER_AGENT)){
			displayType=rb.getByLocale("label_type_superAgent", NodeConfig.getDefaultLocale("messages"));
		}else if(merchant.getType().equals(Merchant.TYPE_EMPLOYEE)
				&& Merchants.hasFlag(merchant,Merchant.FLAG_VIRTUAL_EMPLOYEE)){
			displayType=rb.getByLocale("label_type_virtualEmployee", NodeConfig.getDefaultLocale("messages"));
		}
		String subject = rb.getByLocale("merchant_delete_subject", NodeConfig.getDefaultLocale("messages"), merchant.getMerchantId());
		String body = rb.getByLocale("merchant_delete_body", NodeConfig.getDefaultLocale("messages"), displayType, merchant.getMerchantId());
		oldMessageService.sendEmail(merchant.getEmail(), subject, body, merchant);
	}
	
	
	@Override
	@RestconTransactional
	public void updateSecurityText(String securityText, UserSession session) throws RestconException {
		
		Merchant m = getMerchant(session.getUserId());
		
		//VIPHQ-64	NF1.05 - Secret Word Requirements
		BigDecimal securityTextMinLength = sysPropService.getSystemPropertyNumber(SystemProperty.PROP_CUSTOMER_SECRET_WORD_MIN_LENGTH, session.getTenantId());
		BigDecimal securityTextMaxLength = sysPropService.getSystemPropertyNumber(SystemProperty.PROP_CUSTOMER_SECRET_WORD_MAX_LENGTH, session.getTenantId());
		
		if(securityTextMinLength != null && securityTextMaxLength!=null) { 
		  if(securityText.length() < securityTextMinLength.intValue() || securityText.length() > securityTextMaxLength.intValue()) 
		    throw new RestconLogicViolation(RestconLogicError.SECURITY_WORD_INVALID_LENGTH);
		
		}
		
		AuditTrail auditTrail = auditTrailService.logEvent(AuditEvent.UPDATE_SECURITY_TEXT, "self change", AuditStatus.SUCCESS, session.getDeviceType(),
				m.getId(), m.getIdType(), m.getId(), m.getIdType(), session.getIpAddress(), null, null, null, null, m.getTenantId());
		
		Map<String, String> propToChange = new HashMap<String, String>(1);
		propToChange.put(MerchantProp.SECURITY_A, securityText);
		merchantPropService.updateOrCreatePropInSet(propToChange, m.getId(), m.getTenantId(), auditTrail);
	}
	
	
	@Override
	@RestconTransactional
	public void requestNewBranch(String actorId, RequestNewBranchRequest request, String ipAddress, UserSession session) throws RestconException {
		
		Merchant actor = getMerchant(actorId);
		
		
		NewBranchRequest nbr = new NewBranchRequest();
		nbr.setActor(actor);
		nbr.setChannel(session.getDeviceType());
		nbr.setIpAddress(ipAddress);
		
		nbr.setBranchName(request.getBranchName());
		nbr.setContactFirstName(request.getContactFirstName());
		nbr.setContactLastName(request.getContactLastName());
		nbr.setContactPhoneNumber(request.getContactPhoneNumber());
		nbr.setContactEmail(request.getContactEmail());
		
		merchantService.requestNewBranch(nbr);
	}
	
	@Override
	public  void phoneNumberUnique(String phoneNumber, String merchantId)
			throws RestconException {
		if (merchantId != null && !merchantId.isEmpty()) {
			Merchant merchant = merchantService.find(merchantId,
					UserTypeEnum.MERCHANT_COMMON_ID);
			if (merchant == null) {
				throw new RestconLogicViolation(
						RestconLogicError.MERCHANT_DOES_NOT_EXIST);
			}
		}
		if (!merchantService.phoneNumberUnique(phoneNumber, merchantId)) {
			throw new RestconLogicViolation(
					RestconLogicError.MERCHANT_PHONE_NUMBER_ALREADY_EXISTS);
		}

	}

	
	public List<String> getMerchantTypes() { 
	  return merchantService.getMerchantTypes();	
	}
	
	public List<BankProfile> getBankProfiles() throws RestconException {
		return merchantService.getBankProfiles();
	}
	
	public List<String> getRoutingNumbers() throws RestconException {
		return merchantService.getRoutingNumbers();
	}
	
	public BankProfile getBankProfileById(long id) throws RestconException {
		return merchantService.getBankProfileById(id);
	}
	
	 public BankProfile getDefaultBank() throws RestconException {
	     return merchantService.getDefaultBank();
	    }

	@Override
	public MerchantLimitsSummary getMerchantLimits(String userId)
			throws RestconLogicViolation {
		
		return merchantService.getMerchantLimits(userId);
	}
	
	private MerchCustomTxnLimit findTxnLimit(String limitType, String txnType, String channel, 
			String billSrvcId, List<MerchCustomTxnLimit> txnLimits){
		
		if(txnLimits == null || txnLimits.size() == 0)
			return null;
		for(MerchCustomTxnLimit limit : txnLimits){
			if(limit.getId().getLimitType().equals(limitType) && limit.getId().getTransactionType().equals(txnType) && limit.getId().getChannel().equals(channel) && limit.getId().getBillSrvcId().equals(billSrvcId)){
				return limit;
			}
		}			
		return null;
	}
	
	private MerchCustomTxnLimit findTxnLimit(String limitType, String txnType, String channel, 
			BigDecimal limitTypeValue, String billSrvcId, List<MerchCustomTxnLimit> txnLimits){
		if(txnLimits == null || txnLimits.size() == 0)
			return null;
		for(MerchCustomTxnLimit limit : txnLimits){
			if(limit.getId().getLimitType().equals(limitType) && limit.getId().getTransactionType().equals(txnType) && limit.getId().getLimitTypeValue().equals(limitTypeValue) && limit.getId().getChannel().equals(channel) && limit.getId().getBillSrvcId().equals(billSrvcId)){
				return limit;
			}
		}			
		return null;
	}
	
	@Transactional
	@Override
	public void createMerchTxnLimit(String merchantId, String limitType, String txnType, String billSrvcId, 
			String channel, String currency, BigDecimal limit, String ipAddress, UserSession userSession) throws RestconLogicViolation {
		
		Merchant merchant = merchantService.find(merchantId, UserTypeEnum.MERCHANT_COMMON_ID);
		if (merchant == null ) {
			throw new RestconLogicViolation(RestconLogicError.MERCHANT_DOES_NOT_EXIST);
		}
		
		BigDecimal limitTypeValue = new BigDecimal("1");
//		Merchant limitMerchant = Merchants.getCustomLimitMerchant(merchant);
		
		List<MerchCustomTxnLimit> txnLimits = null;
		if( merchant != null )
			txnLimits = merchant.getMerchCustomTxnLimits();
		
		MerchCustomTxnLimit otherLimit = null;

		if(limitType.contains("txn")){
			otherLimit = findTxnLimit(limitType, txnType, channel, billSrvcId, txnLimits);
			if(otherLimit != null){
				throw new RestconLogicViolation(RestconLogicError.MERCHANT_TXN_LIMIT_ALREADY_EXISTS);
			}
			if(limitType.equals(MerchCatTxnLimit.LIMIT_TYPE_MIN_RCV_PER_TXN)){
				otherLimit = findTxnLimit(MerchCatTxnLimit.LIMIT_TYPE_MAX_RCV_PER_TXN, txnType, channel, billSrvcId, txnLimits);
				if(otherLimit != null && otherLimit.getValue0().compareTo(limit) < 0){
					throw new RestconLogicViolation(RestconLogicError.MERCHANT_TXN_LIMIT_ALREADY_EXISTS);
				}
			}
			else if(limitType.equals(MerchCatTxnLimit.LIMIT_TYPE_MAX_RCV_PER_TXN)){
				otherLimit = findTxnLimit(MerchCatTxnLimit.LIMIT_TYPE_MIN_RCV_PER_TXN, txnType, channel, billSrvcId, txnLimits);
				if(otherLimit != null && otherLimit.getValue0().compareTo(limit) > 0){
					throw new RestconLogicViolation(RestconLogicError.MERCHANT_TXN_LIMIT_ALREADY_EXISTS);
				}
			}
			else if(limitType.equals(MerchCatTxnLimit.LIMIT_TYPE_MIN_SENT_PER_TXN)){
				otherLimit = findTxnLimit(MerchCatTxnLimit.LIMIT_TYPE_MAX_SENT_PER_TXN, txnType, channel, billSrvcId, txnLimits);
				if(otherLimit != null && otherLimit.getValue0().compareTo(limit) < 0){
					throw new RestconLogicViolation(RestconLogicError.MERCHANT_TXN_LIMIT_ALREADY_EXISTS);
				}
			}
			else if(limitType.equals(MerchCatTxnLimit.LIMIT_TYPE_MAX_SENT_PER_TXN)){
				otherLimit = findTxnLimit(MerchCatTxnLimit.LIMIT_TYPE_MIN_SENT_PER_TXN, txnType, channel, billSrvcId, txnLimits);
				if(otherLimit != null && otherLimit.getValue0().compareTo(limit) > 0){
					throw new RestconLogicViolation(RestconLogicError.MERCHANT_TXN_LIMIT_ALREADY_EXISTS);
				}
			}
		}
		else{
			otherLimit = findTxnLimit(limitType, txnType, channel, limitTypeValue, billSrvcId, txnLimits);
			if(otherLimit != null){
				throw new RestconLogicViolation(RestconLogicError.MERCHANT_TXN_LIMIT_ALREADY_EXISTS);
			}
		}

		List<MerchCustomTxnLimit> newtxnLimits = new ArrayList<MerchCustomTxnLimit>();
		MerchCustomTxnLimit newLimit = new MerchCustomTxnLimit();
		MerchCustomTxnLimitId newLimitId = new MerchCustomTxnLimitId();
		newLimit.setId(newLimitId);
		newLimit.setValue0(limit);
		newLimitId.setLimitType(limitType);
		newLimitId.setTransactionType(txnType);
		newLimitId.setId(merchant.getId());
		
		if(channel == null || channel.equals(""))
			newLimitId.setChannel("*");
		else
			newLimitId.setChannel(channel);
		
		newLimitId.setLimitTypeValue(new BigDecimal("1"));
		newLimitId.setBillSrvcId(BillingServiceEnum.valueOf(billSrvcId));
		
		if(currency == null || currency.equals(""))
			newLimit.setCurrency(NodeConfig.getDefaultCurrency());
		else
			newLimit.setCurrency(currency);
		
		newLimit.setTenantId(merchant.getTenantId());
		newLimit.setAuditFreetext("aui");
		
		Merchant merchantToUpdate = getMerchantTenantSafe(merchantId,merchant.getTenantId());
		//audit trail
		AuditTrail auditTrail = auditTrailService.logEvent(
				AuditEvent.CREATE_MERCHANT_TXN_LIMIT, null, AuditStatus.SUCCESS,
				userSession.getDeviceType(), merchant.getId(), merchant.getIdType(),
				merchantToUpdate.getId(), merchantToUpdate.getIdType(),
				ipAddress, null, null, null, null, merchant.getTenantId());
		
		newLimit.setAuditTrail(auditTrail);
		
		newtxnLimits.add(newLimit);
		merchantService.createTxnLimits(merchant, newtxnLimits);
	}
	
	@Transactional
	@Override
	public void createMerchWalletLimit(String merchantId, String walletName, String currency, BigDecimal minBal, BigDecimal maxBal, 
			String ipAddress, UserSession userSession) throws RestconLogicViolation {
		
		Merchant merchant = merchantService.find(merchantId, UserTypeEnum.MERCHANT_COMMON_ID);
		MerchCustomWalletLimit otherLimit = null;
		WalletType walletLimitType = WalletType.valueOf(walletName);
		
//		Merchant limitMerchant = Merchants.getCustomLimitMerchant(merchant);
		
		List<MerchCustomWalletLimit> walletLimits = null;
		if( merchant != null )
			walletLimits = merchant.getMerchCustomWalletLimits();

		otherLimit = findWalletLimit(walletLimitType, walletLimits);
		if(otherLimit != null){
			throw new RestconLogicViolation(RestconLogicError.MERCHANT_WALLET_LIMIT_ALREADY_EXISTS);
		}

		MerchCustomWalletLimit newLimit = new MerchCustomWalletLimit();
		MerchCustomWalletLimitId newLimitId = new MerchCustomWalletLimitId();
		newLimit.setId(newLimitId);
		newLimit.setMaxBalance(maxBal==null?null:maxBal);
		newLimit.setMinBalance(minBal==null?null:minBal);
		newLimit.setCurrency(currency);
		newLimit.setTenantId(merchant.getTenantId());
		newLimitId.setType(walletLimitType);
		String str = merchant.getId();
		newLimitId.setId(str);
		
		Merchant merchantToUpdate = getMerchantTenantSafe(merchantId,merchant.getTenantId());
		//audit trail
		AuditTrail auditTrail = auditTrailService.logEvent(
				AuditEvent.CREATE_MERCHANT_WALLET_LIMIT, null, AuditStatus.SUCCESS,
				userSession.getDeviceType(), merchant.getId(), merchant.getIdType(),
				merchantToUpdate.getId(), merchantToUpdate.getIdType(),
				ipAddress, null, null, null, null, merchant.getTenantId());
		newLimit.setAuditTrail(auditTrail);
		newLimit.setAuditFreetext("aui");
		
		List<MerchCustomWalletLimit> newWalletLimits = new ArrayList<MerchCustomWalletLimit>();
		newWalletLimits.add(newLimit);
		
		merchantService.createWalletLimits(merchant, newWalletLimits);
	}
	
	@Override
	@RestconTransactional
	public Response getRfpTransactions(int firstRow, int maxRows, boolean count, String merchantId,
			String searchDirection, String searchStatus, String searchTxnId, BigDecimal searchMinAmount,
			BigDecimal searchMaxAmount, String startDate, String endDate) throws RestconException {
	 
		Merchant merchant = getMerchant(merchantId);
		String surId = merchant.getId();
		
		boolean searchIncludeNonFail = true;
		boolean searchIncludeFail = true;
		
		if(!(searchStatus == null || searchStatus.equals(""))){
			if(searchStatus.startsWith("f"))
				searchIncludeNonFail = false; //only include failures
			else
				searchIncludeFail = false; //only include non failures
		}
		
		Date startDt = null;
		Date endDt = null;
		if (startDate != null) {
			try {
				startDt = new SimpleDateFormat("yyyy-MM-dd").parse(startDate);

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RestconLogicViolation(RestconLogicError.DATE_INVALID);
			}
		}

		if (endDate != null) {
			try {
				endDt = new SimpleDateFormat("yyyy-MM-dd").parse(endDate);

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RestconLogicViolation(RestconLogicError.DATE_INVALID);
			}
		}
		
		MWalletPrice minAmt = null;
		MWalletPrice maxAmt = null;
		if (searchMinAmount != null) {
			try {
				minAmt = new MWalletPrice(searchMinAmount, NodeConfig.getDefaultCurrency());
				
				if (searchMaxAmount != null)
					maxAmt = new MWalletPrice(searchMaxAmount, NodeConfig.getDefaultCurrency());
		
			} catch (InvalidPriceException e) {
				e.printStackTrace();
			} catch (UnrecognizedCurrencyException e) {
				e.printStackTrace();
			}
		}

		List list = merchantService.getRfpTransactions(firstRow, maxRows, count, surId, merchantId, 
				searchDirection, searchStatus, searchTxnId, minAmt, maxAmt, searchIncludeNonFail, 
				searchIncludeFail, startDt, endDt);
		
		
		if(count) {
			
			CountResponse  c  = new CountResponse(); 
			c.setCount(((BigDecimal)list.get(0)).intValue());
			return Response.ok().entity(c).build();
			
		}else {
		List responseList = new ArrayList();
		for (int i = 0; i < list.size(); i++) {
			BillTxn billTxn = (BillTxn) list.get(i);
			BillTxnResponse billTxnResponse = new BillTxnResponse();
			billTxnResponse.setBillSrvcId(billTxn.getBillSrvcId());
			// billTxnResponse.setBillTxnCommission(billTxn.getBillTxnCommission());

			BillTxnCommission billTxnCommission = billTxn.getBillTxnCommission();
			if (billTxnCommission != null) {
				BillTxnCommissionResponse billTxnCommissionResponse = new BillTxnCommissionResponse();
				billTxnCommissionResponse.setAcomm(billTxnCommission.getAcomm());
				billTxnCommissionResponse.setAcommNotPaid(billTxnCommission.getAcommNotPaid());
				billTxnCommissionResponse.setCsas(billTxnCommission.getCsas());
				billTxnCommissionResponse.setCsasNotPaid(billTxnCommission.getCsasNotPaid());
				billTxnCommissionResponse.setGe(billTxnCommission.getGe());
				billTxnCommissionResponse.setGeNotPaid(billTxnCommission.getGeNotPaid());
				billTxnCommissionResponse.setRb(billTxnCommission.getRb());
				billTxnCommissionResponse.setRbNotPaid(billTxnCommission.getRbNotPaid());
				billTxnCommissionResponse.setSystem(billTxnCommission.getSystem());
				billTxnCommissionResponse.setSystemNotPaid(billTxnCommission.getSystemNotPaid());
				billTxnCommissionResponse.setTelefonica(billTxnCommission.getTelefonica());
				billTxnCommissionResponse.setTelefonicaNotPaid(billTxnCommission.getTelefonicaNotPaid());
				billTxnCommissionResponse.setTmobile(billTxnCommission.getTmobile());
				billTxnCommissionResponse.setTmobileNotPaid(billTxnCommission.getTmobileNotPaid());
				billTxnCommissionResponse.setUcb(billTxnCommission.getUcb());
				billTxnCommissionResponse.setUcbNotPaid(billTxnCommission.getUcbNotPaid());
				billTxnCommissionResponse.setVersion(billTxnCommission.getVersion());
				billTxnCommissionResponse.setVodafone(billTxnCommission.getVodafone());
				billTxnCommissionResponse.setVodafoneNotPaid(billTxnCommission.getVodafoneNotPaid());
				billTxnResponse.setBillTxnCommission(billTxnCommissionResponse);
			}

			BillTxnFailure billTxnFailure = billTxn.getBillTxnFailure();
			if(billTxnFailure != null){
			BillTxnFailureResponse billTxnFailureResponse = new BillTxnFailureResponse();
			billTxnFailureResponse.setFlags(billTxnFailure.getFlags());
			billTxnFailureResponse.setFreetext(billTxnFailure.getFreetext());
			billTxnFailureResponse.setGroupId(billTxnFailure.getGroupId());
			billTxnFailureResponse.setKey(billTxnFailure.getKey());
			billTxnFailureResponse.setMessage(billTxnFailure.getKey());
			billTxnFailureResponse.setMessageAux(billTxnFailure.getMessageAux());
			billTxnFailureResponse.setMsgId(billTxnFailure.getMsgId());
			billTxnFailureResponse.setVersion(billTxnFailure.getVersion());
			billTxnResponse.setBillTxnFailure(billTxnFailureResponse);
			}

			BillTxnFundout billTxnFundout = billTxn.getBillTxnFundout();
			if(billTxnFundout != null) {
				BillTxnFundoutResponse billTxnFundoutResponse = new BillTxnFundoutResponse();
			billTxnFundoutResponse.setFlags(billTxnFundout.getFlags());
			billTxnFundoutResponse.setPayaggrdate(billTxnFundout.getPayaggrdate());
			billTxnFundoutResponse.setPayorderdate(billTxnFundout.getPayorderdate());
			billTxnFundoutResponse.setPayorderstatus(billTxnFundout.getPayorderstatus());
			billTxnFundoutResponse.setPuLink(billTxnFundout.getPuLink());
			billTxnFundoutResponse.setTrnLink(billTxnFundout.getTrnLink());
			billTxnFundoutResponse.setVersion(billTxnFundout.getVersion());
			billTxnResponse.setBillTxnFundout(billTxnFundoutResponse);
			billTxnResponse.setBillTxnFundout(billTxnFundoutResponse);
			}

			Set set = billTxn.getBillTxnProps();
			if(set != null) {
			Set newSet = new HashSet();
			Iterator it = set.iterator();

			while (it.hasNext()) {

				BillTxnProp billTxnProp = (BillTxnProp) it.next();
				BillTxnPropResponse billTxnPropResponse = new BillTxnPropResponse();
				billTxnPropResponse.setPropValue(billTxnProp.getPropValue());
				billTxnPropResponse.setVersion(billTxnProp.getVersion());
				billTxnPropResponse.setPropName(billTxnProp.getPropName());
				newSet.add(billTxnPropResponse);

			}

			billTxnResponse.setBillTxnProps(newSet);
			}
			
			billTxnResponse.setChannel(billTxn.getChannel());
			billTxnResponse.setDestId(billTxn.getDestId());
			billTxnResponse.setDestMsisdn(billTxn.getDestMsisdn());
			billTxnResponse.setDestType(billTxn.getDestType());
			billTxnResponse.setFlags(billTxn.getFlags());
			billTxnResponse.setMultimodeDest(billTxn.getMultimodeDest());
			billTxnResponse.setMultimodeSrc(billTxn.getMultimodeSrc());
			billTxnResponse.setPrice(billTxn.getPrice());
			billTxnResponse.setPriceAml(billTxn.getPriceAml());
			billTxnResponse.setPriceCrncy(billTxn.getPriceCrncy());
			billTxnResponse.setReportingDatetime(billTxn.getReportingDatetime());
			billTxnResponse.setReportingState(billTxn.getReportingState());
			billTxnResponse.setSrcId(billTxn.getSrcId());
			billTxnResponse.setSrcMsisdn(billTxn.getSrcMsisdn());
			billTxnResponse.setSrcType(billTxn.getSrcType());
			billTxnResponse.setBillSrvcId(billTxn.getBillSrvcId());
			billTxnResponse.setTxnId(billTxn.getTxnId());
			billTxnResponse.setType(billTxn.getType());
			billTxnResponse.setVersion(billTxn.getVersion());
			responseList.add(billTxnResponse);

		}
		
		return Response.ok().entity(responseList).build();
	}
 }
	
	private MerchCustomWalletLimit findWalletLimit(WalletType type, List<MerchCustomWalletLimit> walletLimits){
		if(walletLimits == null || walletLimits.size() == 0)
			return null;
		for(MerchCustomWalletLimit limit : walletLimits){
			if(limit.getType() == type){
				return limit;
			}
		}			
		return null;
	}
	
	@Override
	@RestconTransactional
	public MerchantBalanceResponse getBalance(String userId) throws RestconException {
		Merchant actor = getMerchant(userId);
		Merchant src = actor;
		if ("multi".equals(merchantPropService.findValue(actor.getId(), MerchantProp.MERCHANT_MODE))) {
			src = actor.getMerchantByParentId();
		}

		MerchantBalanceResponse response = new MerchantBalanceResponse();
		BigDecimal balVip = merchantService.getMerchantEstimateBalance(src);
		response.setMerchBal(balVip);
		response.setMerchBalUpdateTimestamp(new Date());
		//response.setFormattedBalance(CurrencyFormatter.convertAmountWithSymbol(balEsti));
		response.setCurrency(NodeConfig.getDefaultCurrency());

		return response;
	}

	@Override
	public MerchantFundoutResponse getFundout(String merchantId) throws RestconException {
		Merchant merchant = getMerchant(merchantId);
		MerchantPropResultSet merchantAllProperties = merchantPropService.findAll(merchant.getId());
		return new MerchantFundoutResponse(merchantAllProperties);
	}

	@Override
	@RestconTransactional
	public void updateFundout(String merchantId, MerchantFundoutRequest merchantFundoutRequest, UserSession userSession, String ipAddress) throws RestconException {
		Merchant merchant = getMerchant(merchantId);
		AuditTrail auditTrail = auditTrailService.logEvent(
				AuditEvent.UPDATE_MERCHANT_FUNDOUT, null, AuditStatus.SUCCESS,
				userSession.getDeviceType(), merchant.getId(), merchant.getIdType(),
				merchant.getId(), merchant.getIdType(),
				ipAddress, null, null, null, null, merchant.getTenantId());
		MerchantPropUpdateExecutor propUpdateExecutor = merchantPropService.propUpdateExecutor(merchant.getId(), merchant.getTenantId(), auditTrail);
		fillMerchantProperties(propUpdateExecutor, merchantFundoutRequest);
		propUpdateExecutor.execute();
	}

	private void fillMerchantProperties(MerchantPropUpdateExecutor propUpdateExecutor, MerchantFundoutRequest merchantFundoutRequest) {
		// setting null for empty values causes deletion entries in database table
		if (merchantFundoutRequest.isMethodToUpdate()) {
			propUpdateExecutor.addProp(MerchantProp.MERCHANT_FUNDOUT_METHOD, merchantFundoutRequest.getMethod());
		}
		if (merchantFundoutRequest.isAllFieldsToUpdate()) {
			propUpdateExecutor.addProp(MerchantProp.MERCHANT_FUNDOUT_AMOUNT, merchantFundoutRequest.getAmount() == null ? null : merchantFundoutRequest.getAmount().toString());
			propUpdateExecutor.addProp(MerchantProp.MERCHANT_FUNDOUT_HOUR_OF_DAY, merchantFundoutRequest.getHourOfDay());
		}
		if (merchantFundoutRequest.isFrequencyToUpdate()) {
			propUpdateExecutor.addProp(MerchantProp.MERCHANT_FUNDOUT_FREQUENCY, merchantFundoutRequest.getFrequency());
		}
	}
}
