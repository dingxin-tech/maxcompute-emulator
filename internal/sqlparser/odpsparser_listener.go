// Code generated from grammar/OdpsParser.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // OdpsParser
import "github.com/antlr4-go/antlr/v4"

// OdpsParserListener is a complete listener for a parse tree produced by OdpsParser.
type OdpsParserListener interface {
	antlr.ParseTreeListener

	// EnterScript is called when entering the script production.
	EnterScript(c *ScriptContext)

	// EnterUserCodeBlock is called when entering the userCodeBlock production.
	EnterUserCodeBlock(c *UserCodeBlockContext)

	// EnterStatement is called when entering the statement production.
	EnterStatement(c *StatementContext)

	// EnterCompoundStatement is called when entering the compoundStatement production.
	EnterCompoundStatement(c *CompoundStatementContext)

	// EnterNoopStatement is called when entering the noopStatement production.
	EnterNoopStatement(c *NoopStatementContext)

	// EnterExecStatement is called when entering the execStatement production.
	EnterExecStatement(c *ExecStatementContext)

	// EnterCteStatement is called when entering the cteStatement production.
	EnterCteStatement(c *CteStatementContext)

	// EnterTableAliasWithCols is called when entering the tableAliasWithCols production.
	EnterTableAliasWithCols(c *TableAliasWithColsContext)

	// EnterSubQuerySource is called when entering the subQuerySource production.
	EnterSubQuerySource(c *SubQuerySourceContext)

	// EnterExplainStatement is called when entering the explainStatement production.
	EnterExplainStatement(c *ExplainStatementContext)

	// EnterIfStatement is called when entering the ifStatement production.
	EnterIfStatement(c *IfStatementContext)

	// EnterLoopStatement is called when entering the loopStatement production.
	EnterLoopStatement(c *LoopStatementContext)

	// EnterFunctionDefinition is called when entering the functionDefinition production.
	EnterFunctionDefinition(c *FunctionDefinitionContext)

	// EnterFunctionParameters is called when entering the functionParameters production.
	EnterFunctionParameters(c *FunctionParametersContext)

	// EnterParameterDefinition is called when entering the parameterDefinition production.
	EnterParameterDefinition(c *ParameterDefinitionContext)

	// EnterTypeDeclaration is called when entering the typeDeclaration production.
	EnterTypeDeclaration(c *TypeDeclarationContext)

	// EnterParameterTypeDeclaration is called when entering the parameterTypeDeclaration production.
	EnterParameterTypeDeclaration(c *ParameterTypeDeclarationContext)

	// EnterFunctionTypeDeclaration is called when entering the functionTypeDeclaration production.
	EnterFunctionTypeDeclaration(c *FunctionTypeDeclarationContext)

	// EnterParameterTypeDeclarationList is called when entering the parameterTypeDeclarationList production.
	EnterParameterTypeDeclarationList(c *ParameterTypeDeclarationListContext)

	// EnterParameterColumnNameTypeList is called when entering the parameterColumnNameTypeList production.
	EnterParameterColumnNameTypeList(c *ParameterColumnNameTypeListContext)

	// EnterParameterColumnNameType is called when entering the parameterColumnNameType production.
	EnterParameterColumnNameType(c *ParameterColumnNameTypeContext)

	// EnterVarSizeParam is called when entering the varSizeParam production.
	EnterVarSizeParam(c *VarSizeParamContext)

	// EnterAssignStatement is called when entering the assignStatement production.
	EnterAssignStatement(c *AssignStatementContext)

	// EnterPreSelectClauses is called when entering the preSelectClauses production.
	EnterPreSelectClauses(c *PreSelectClausesContext)

	// EnterPostSelectClauses is called when entering the postSelectClauses production.
	EnterPostSelectClauses(c *PostSelectClausesContext)

	// EnterSelectRest is called when entering the selectRest production.
	EnterSelectRest(c *SelectRestContext)

	// EnterMultiInsertFromRest is called when entering the multiInsertFromRest production.
	EnterMultiInsertFromRest(c *MultiInsertFromRestContext)

	// EnterFromRest is called when entering the fromRest production.
	EnterFromRest(c *FromRestContext)

	// EnterSimpleQueryExpression is called when entering the simpleQueryExpression production.
	EnterSimpleQueryExpression(c *SimpleQueryExpressionContext)

	// EnterSelectQueryExpression is called when entering the selectQueryExpression production.
	EnterSelectQueryExpression(c *SelectQueryExpressionContext)

	// EnterFromQueryExpression is called when entering the fromQueryExpression production.
	EnterFromQueryExpression(c *FromQueryExpressionContext)

	// EnterSetOperationFactor is called when entering the setOperationFactor production.
	EnterSetOperationFactor(c *SetOperationFactorContext)

	// EnterQueryExpression is called when entering the queryExpression production.
	EnterQueryExpression(c *QueryExpressionContext)

	// EnterQueryExpressionWithCTE is called when entering the queryExpressionWithCTE production.
	EnterQueryExpressionWithCTE(c *QueryExpressionWithCTEContext)

	// EnterSetRHS is called when entering the setRHS production.
	EnterSetRHS(c *SetRHSContext)

	// EnterMultiInsertSetOperationFactor is called when entering the multiInsertSetOperationFactor production.
	EnterMultiInsertSetOperationFactor(c *MultiInsertSetOperationFactorContext)

	// EnterMultiInsertSelect is called when entering the multiInsertSelect production.
	EnterMultiInsertSelect(c *MultiInsertSelectContext)

	// EnterMultiInsertSetRHS is called when entering the multiInsertSetRHS production.
	EnterMultiInsertSetRHS(c *MultiInsertSetRHSContext)

	// EnterMultiInsertBranch is called when entering the multiInsertBranch production.
	EnterMultiInsertBranch(c *MultiInsertBranchContext)

	// EnterFromStatement is called when entering the fromStatement production.
	EnterFromStatement(c *FromStatementContext)

	// EnterInsertStatement is called when entering the insertStatement production.
	EnterInsertStatement(c *InsertStatementContext)

	// EnterSelectQueryStatement is called when entering the selectQueryStatement production.
	EnterSelectQueryStatement(c *SelectQueryStatementContext)

	// EnterQueryStatement is called when entering the queryStatement production.
	EnterQueryStatement(c *QueryStatementContext)

	// EnterInsertStatementWithCTE is called when entering the insertStatementWithCTE production.
	EnterInsertStatementWithCTE(c *InsertStatementWithCTEContext)

	// EnterSubQueryExpression is called when entering the subQueryExpression production.
	EnterSubQueryExpression(c *SubQueryExpressionContext)

	// EnterLimitClause is called when entering the limitClause production.
	EnterLimitClause(c *LimitClauseContext)

	// EnterFromSource is called when entering the fromSource production.
	EnterFromSource(c *FromSourceContext)

	// EnterTableVariableSource is called when entering the tableVariableSource production.
	EnterTableVariableSource(c *TableVariableSourceContext)

	// EnterTableFunctionSource is called when entering the tableFunctionSource production.
	EnterTableFunctionSource(c *TableFunctionSourceContext)

	// EnterCreateMachineLearningModelStatment is called when entering the createMachineLearningModelStatment production.
	EnterCreateMachineLearningModelStatment(c *CreateMachineLearningModelStatmentContext)

	// EnterVariableName is called when entering the variableName production.
	EnterVariableName(c *VariableNameContext)

	// EnterAtomExpression is called when entering the atomExpression production.
	EnterAtomExpression(c *AtomExpressionContext)

	// EnterVariableRef is called when entering the variableRef production.
	EnterVariableRef(c *VariableRefContext)

	// EnterVariableCall is called when entering the variableCall production.
	EnterVariableCall(c *VariableCallContext)

	// EnterFunNameRef is called when entering the funNameRef production.
	EnterFunNameRef(c *FunNameRefContext)

	// EnterLambdaExpression is called when entering the lambdaExpression production.
	EnterLambdaExpression(c *LambdaExpressionContext)

	// EnterLambdaParameter is called when entering the lambdaParameter production.
	EnterLambdaParameter(c *LambdaParameterContext)

	// EnterTableOrColumnRef is called when entering the tableOrColumnRef production.
	EnterTableOrColumnRef(c *TableOrColumnRefContext)

	// EnterConstructExpression is called when entering the constructExpression production.
	EnterConstructExpression(c *ConstructExpressionContext)

	// EnterExistsExpression is called when entering the existsExpression production.
	EnterExistsExpression(c *ExistsExpressionContext)

	// EnterScalarSubQueryExpression is called when entering the scalarSubQueryExpression production.
	EnterScalarSubQueryExpression(c *ScalarSubQueryExpressionContext)

	// EnterClassNameWithPackage is called when entering the classNameWithPackage production.
	EnterClassNameWithPackage(c *ClassNameWithPackageContext)

	// EnterClassNameOrArrayDecl is called when entering the classNameOrArrayDecl production.
	EnterClassNameOrArrayDecl(c *ClassNameOrArrayDeclContext)

	// EnterClassNameList is called when entering the classNameList production.
	EnterClassNameList(c *ClassNameListContext)

	// EnterOdpsqlNonReserved is called when entering the odpsqlNonReserved production.
	EnterOdpsqlNonReserved(c *OdpsqlNonReservedContext)

	// EnterRelaxedKeywords is called when entering the relaxedKeywords production.
	EnterRelaxedKeywords(c *RelaxedKeywordsContext)

	// EnterAllIdentifiers is called when entering the allIdentifiers production.
	EnterAllIdentifiers(c *AllIdentifiersContext)

	// EnterIdentifier is called when entering the identifier production.
	EnterIdentifier(c *IdentifierContext)

	// EnterAliasIdentifier is called when entering the aliasIdentifier production.
	EnterAliasIdentifier(c *AliasIdentifierContext)

	// EnterIdentifierWithoutSql11 is called when entering the identifierWithoutSql11 production.
	EnterIdentifierWithoutSql11(c *IdentifierWithoutSql11Context)

	// EnterAlterTableChangeOwner is called when entering the alterTableChangeOwner production.
	EnterAlterTableChangeOwner(c *AlterTableChangeOwnerContext)

	// EnterAlterViewChangeOwner is called when entering the alterViewChangeOwner production.
	EnterAlterViewChangeOwner(c *AlterViewChangeOwnerContext)

	// EnterAlterTableEnableHubTable is called when entering the alterTableEnableHubTable production.
	EnterAlterTableEnableHubTable(c *AlterTableEnableHubTableContext)

	// EnterTableLifecycle is called when entering the tableLifecycle production.
	EnterTableLifecycle(c *TableLifecycleContext)

	// EnterSetStatement is called when entering the setStatement production.
	EnterSetStatement(c *SetStatementContext)

	// EnterAnythingButEqualOrSemi is called when entering the anythingButEqualOrSemi production.
	EnterAnythingButEqualOrSemi(c *AnythingButEqualOrSemiContext)

	// EnterAnythingButSemi is called when entering the anythingButSemi production.
	EnterAnythingButSemi(c *AnythingButSemiContext)

	// EnterSetProjectStatement is called when entering the setProjectStatement production.
	EnterSetProjectStatement(c *SetProjectStatementContext)

	// EnterLabel is called when entering the label production.
	EnterLabel(c *LabelContext)

	// EnterSkewInfoVal is called when entering the skewInfoVal production.
	EnterSkewInfoVal(c *SkewInfoValContext)

	// EnterMemberAccessOperator is called when entering the memberAccessOperator production.
	EnterMemberAccessOperator(c *MemberAccessOperatorContext)

	// EnterMethodAccessOperator is called when entering the methodAccessOperator production.
	EnterMethodAccessOperator(c *MethodAccessOperatorContext)

	// EnterIsNullOperator is called when entering the isNullOperator production.
	EnterIsNullOperator(c *IsNullOperatorContext)

	// EnterInOperator is called when entering the inOperator production.
	EnterInOperator(c *InOperatorContext)

	// EnterBetweenOperator is called when entering the betweenOperator production.
	EnterBetweenOperator(c *BetweenOperatorContext)

	// EnterMathExpression is called when entering the mathExpression production.
	EnterMathExpression(c *MathExpressionContext)

	// EnterUnarySuffixExpression is called when entering the unarySuffixExpression production.
	EnterUnarySuffixExpression(c *UnarySuffixExpressionContext)

	// EnterUnaryPrefixExpression is called when entering the unaryPrefixExpression production.
	EnterUnaryPrefixExpression(c *UnaryPrefixExpressionContext)

	// EnterFieldExpression is called when entering the fieldExpression production.
	EnterFieldExpression(c *FieldExpressionContext)

	// EnterLogicalExpression is called when entering the logicalExpression production.
	EnterLogicalExpression(c *LogicalExpressionContext)

	// EnterNotExpression is called when entering the notExpression production.
	EnterNotExpression(c *NotExpressionContext)

	// EnterEqualExpression is called when entering the equalExpression production.
	EnterEqualExpression(c *EqualExpressionContext)

	// EnterMathExpressionListInParentheses is called when entering the mathExpressionListInParentheses production.
	EnterMathExpressionListInParentheses(c *MathExpressionListInParenthesesContext)

	// EnterMathExpressionList is called when entering the mathExpressionList production.
	EnterMathExpressionList(c *MathExpressionListContext)

	// EnterExpression is called when entering the expression production.
	EnterExpression(c *ExpressionContext)

	// EnterStatisticStatement is called when entering the statisticStatement production.
	EnterStatisticStatement(c *StatisticStatementContext)

	// EnterAddRemoveStatisticStatement is called when entering the addRemoveStatisticStatement production.
	EnterAddRemoveStatisticStatement(c *AddRemoveStatisticStatementContext)

	// EnterStatisticInfo is called when entering the statisticInfo production.
	EnterStatisticInfo(c *StatisticInfoContext)

	// EnterShowStatisticStatement is called when entering the showStatisticStatement production.
	EnterShowStatisticStatement(c *ShowStatisticStatementContext)

	// EnterShowStatisticListStatement is called when entering the showStatisticListStatement production.
	EnterShowStatisticListStatement(c *ShowStatisticListStatementContext)

	// EnterCountTableStatement is called when entering the countTableStatement production.
	EnterCountTableStatement(c *CountTableStatementContext)

	// EnterStatisticName is called when entering the statisticName production.
	EnterStatisticName(c *StatisticNameContext)

	// EnterInstanceManagement is called when entering the instanceManagement production.
	EnterInstanceManagement(c *InstanceManagementContext)

	// EnterInstanceStatus is called when entering the instanceStatus production.
	EnterInstanceStatus(c *InstanceStatusContext)

	// EnterKillInstance is called when entering the killInstance production.
	EnterKillInstance(c *KillInstanceContext)

	// EnterInstanceId is called when entering the instanceId production.
	EnterInstanceId(c *InstanceIdContext)

	// EnterResourceManagement is called when entering the resourceManagement production.
	EnterResourceManagement(c *ResourceManagementContext)

	// EnterAddResource is called when entering the addResource production.
	EnterAddResource(c *AddResourceContext)

	// EnterDropResource is called when entering the dropResource production.
	EnterDropResource(c *DropResourceContext)

	// EnterResourceId is called when entering the resourceId production.
	EnterResourceId(c *ResourceIdContext)

	// EnterDropOfflineModel is called when entering the dropOfflineModel production.
	EnterDropOfflineModel(c *DropOfflineModelContext)

	// EnterGetResource is called when entering the getResource production.
	EnterGetResource(c *GetResourceContext)

	// EnterOptions is called when entering the options production.
	EnterOptions(c *OptionsContext)

	// EnterAuthorizationStatement is called when entering the authorizationStatement production.
	EnterAuthorizationStatement(c *AuthorizationStatementContext)

	// EnterListUsers is called when entering the listUsers production.
	EnterListUsers(c *ListUsersContext)

	// EnterListGroups is called when entering the listGroups production.
	EnterListGroups(c *ListGroupsContext)

	// EnterAddUserStatement is called when entering the addUserStatement production.
	EnterAddUserStatement(c *AddUserStatementContext)

	// EnterAddGroupStatement is called when entering the addGroupStatement production.
	EnterAddGroupStatement(c *AddGroupStatementContext)

	// EnterRemoveUserStatement is called when entering the removeUserStatement production.
	EnterRemoveUserStatement(c *RemoveUserStatementContext)

	// EnterRemoveGroupStatement is called when entering the removeGroupStatement production.
	EnterRemoveGroupStatement(c *RemoveGroupStatementContext)

	// EnterAddAccountProvider is called when entering the addAccountProvider production.
	EnterAddAccountProvider(c *AddAccountProviderContext)

	// EnterRemoveAccountProvider is called when entering the removeAccountProvider production.
	EnterRemoveAccountProvider(c *RemoveAccountProviderContext)

	// EnterShowAcl is called when entering the showAcl production.
	EnterShowAcl(c *ShowAclContext)

	// EnterListRoles is called when entering the listRoles production.
	EnterListRoles(c *ListRolesContext)

	// EnterWhoami is called when entering the whoami production.
	EnterWhoami(c *WhoamiContext)

	// EnterListTrustedProjects is called when entering the listTrustedProjects production.
	EnterListTrustedProjects(c *ListTrustedProjectsContext)

	// EnterAddTrustedProject is called when entering the addTrustedProject production.
	EnterAddTrustedProject(c *AddTrustedProjectContext)

	// EnterRemoveTrustedProject is called when entering the removeTrustedProject production.
	EnterRemoveTrustedProject(c *RemoveTrustedProjectContext)

	// EnterShowSecurityConfiguration is called when entering the showSecurityConfiguration production.
	EnterShowSecurityConfiguration(c *ShowSecurityConfigurationContext)

	// EnterShowPackages is called when entering the showPackages production.
	EnterShowPackages(c *ShowPackagesContext)

	// EnterShowItems is called when entering the showItems production.
	EnterShowItems(c *ShowItemsContext)

	// EnterInstallPackage is called when entering the installPackage production.
	EnterInstallPackage(c *InstallPackageContext)

	// EnterUninstallPackage is called when entering the uninstallPackage production.
	EnterUninstallPackage(c *UninstallPackageContext)

	// EnterCreatePackage is called when entering the createPackage production.
	EnterCreatePackage(c *CreatePackageContext)

	// EnterDeletePackage is called when entering the deletePackage production.
	EnterDeletePackage(c *DeletePackageContext)

	// EnterAddToPackage is called when entering the addToPackage production.
	EnterAddToPackage(c *AddToPackageContext)

	// EnterRemoveFromPackage is called when entering the removeFromPackage production.
	EnterRemoveFromPackage(c *RemoveFromPackageContext)

	// EnterAllowPackage is called when entering the allowPackage production.
	EnterAllowPackage(c *AllowPackageContext)

	// EnterDisallowPackage is called when entering the disallowPackage production.
	EnterDisallowPackage(c *DisallowPackageContext)

	// EnterPutPolicy is called when entering the putPolicy production.
	EnterPutPolicy(c *PutPolicyContext)

	// EnterGetPolicy is called when entering the getPolicy production.
	EnterGetPolicy(c *GetPolicyContext)

	// EnterClearExpiredGrants is called when entering the clearExpiredGrants production.
	EnterClearExpiredGrants(c *ClearExpiredGrantsContext)

	// EnterGrantLabel is called when entering the grantLabel production.
	EnterGrantLabel(c *GrantLabelContext)

	// EnterRevokeLabel is called when entering the revokeLabel production.
	EnterRevokeLabel(c *RevokeLabelContext)

	// EnterShowLabel is called when entering the showLabel production.
	EnterShowLabel(c *ShowLabelContext)

	// EnterGrantSuperPrivilege is called when entering the grantSuperPrivilege production.
	EnterGrantSuperPrivilege(c *GrantSuperPrivilegeContext)

	// EnterRevokeSuperPrivilege is called when entering the revokeSuperPrivilege production.
	EnterRevokeSuperPrivilege(c *RevokeSuperPrivilegeContext)

	// EnterCreateRoleStatement is called when entering the createRoleStatement production.
	EnterCreateRoleStatement(c *CreateRoleStatementContext)

	// EnterDropRoleStatement is called when entering the dropRoleStatement production.
	EnterDropRoleStatement(c *DropRoleStatementContext)

	// EnterAddRoleToProject is called when entering the addRoleToProject production.
	EnterAddRoleToProject(c *AddRoleToProjectContext)

	// EnterRemoveRoleFromProject is called when entering the removeRoleFromProject production.
	EnterRemoveRoleFromProject(c *RemoveRoleFromProjectContext)

	// EnterGrantRole is called when entering the grantRole production.
	EnterGrantRole(c *GrantRoleContext)

	// EnterRevokeRole is called when entering the revokeRole production.
	EnterRevokeRole(c *RevokeRoleContext)

	// EnterGrantPrivileges is called when entering the grantPrivileges production.
	EnterGrantPrivileges(c *GrantPrivilegesContext)

	// EnterPrivilegeProperties is called when entering the privilegeProperties production.
	EnterPrivilegeProperties(c *PrivilegePropertiesContext)

	// EnterPrivilegePropertieKeys is called when entering the privilegePropertieKeys production.
	EnterPrivilegePropertieKeys(c *PrivilegePropertieKeysContext)

	// EnterRevokePrivileges is called when entering the revokePrivileges production.
	EnterRevokePrivileges(c *RevokePrivilegesContext)

	// EnterPurgePrivileges is called when entering the purgePrivileges production.
	EnterPurgePrivileges(c *PurgePrivilegesContext)

	// EnterShowGrants is called when entering the showGrants production.
	EnterShowGrants(c *ShowGrantsContext)

	// EnterShowRoleGrants is called when entering the showRoleGrants production.
	EnterShowRoleGrants(c *ShowRoleGrantsContext)

	// EnterShowRoles is called when entering the showRoles production.
	EnterShowRoles(c *ShowRolesContext)

	// EnterShowRolePrincipals is called when entering the showRolePrincipals production.
	EnterShowRolePrincipals(c *ShowRolePrincipalsContext)

	// EnterUser is called when entering the user production.
	EnterUser(c *UserContext)

	// EnterUserRoleComments is called when entering the userRoleComments production.
	EnterUserRoleComments(c *UserRoleCommentsContext)

	// EnterAccountProvider is called when entering the accountProvider production.
	EnterAccountProvider(c *AccountProviderContext)

	// EnterProjectName is called when entering the projectName production.
	EnterProjectName(c *ProjectNameContext)

	// EnterPrivilegeObjectName is called when entering the privilegeObjectName production.
	EnterPrivilegeObjectName(c *PrivilegeObjectNameContext)

	// EnterPrivilegeObjectType is called when entering the privilegeObjectType production.
	EnterPrivilegeObjectType(c *PrivilegeObjectTypeContext)

	// EnterRoleName is called when entering the roleName production.
	EnterRoleName(c *RoleNameContext)

	// EnterPackageName is called when entering the packageName production.
	EnterPackageName(c *PackageNameContext)

	// EnterPackageNameWithProject is called when entering the packageNameWithProject production.
	EnterPackageNameWithProject(c *PackageNameWithProjectContext)

	// EnterPrincipalSpecification is called when entering the principalSpecification production.
	EnterPrincipalSpecification(c *PrincipalSpecificationContext)

	// EnterPrincipalName is called when entering the principalName production.
	EnterPrincipalName(c *PrincipalNameContext)

	// EnterPrincipalIdentifier is called when entering the principalIdentifier production.
	EnterPrincipalIdentifier(c *PrincipalIdentifierContext)

	// EnterPrivilege is called when entering the privilege production.
	EnterPrivilege(c *PrivilegeContext)

	// EnterPrivilegeType is called when entering the privilegeType production.
	EnterPrivilegeType(c *PrivilegeTypeContext)

	// EnterPrivilegeObject is called when entering the privilegeObject production.
	EnterPrivilegeObject(c *PrivilegeObjectContext)

	// EnterFilePath is called when entering the filePath production.
	EnterFilePath(c *FilePathContext)

	// EnterPolicyCondition is called when entering the policyCondition production.
	EnterPolicyCondition(c *PolicyConditionContext)

	// EnterPolicyConditionOp is called when entering the policyConditionOp production.
	EnterPolicyConditionOp(c *PolicyConditionOpContext)

	// EnterPolicyKey is called when entering the policyKey production.
	EnterPolicyKey(c *PolicyKeyContext)

	// EnterPolicyValue is called when entering the policyValue production.
	EnterPolicyValue(c *PolicyValueContext)

	// EnterShowCurrentRole is called when entering the showCurrentRole production.
	EnterShowCurrentRole(c *ShowCurrentRoleContext)

	// EnterSetRole is called when entering the setRole production.
	EnterSetRole(c *SetRoleContext)

	// EnterAdminOptionFor is called when entering the adminOptionFor production.
	EnterAdminOptionFor(c *AdminOptionForContext)

	// EnterWithAdminOption is called when entering the withAdminOption production.
	EnterWithAdminOption(c *WithAdminOptionContext)

	// EnterWithGrantOption is called when entering the withGrantOption production.
	EnterWithGrantOption(c *WithGrantOptionContext)

	// EnterGrantOptionFor is called when entering the grantOptionFor production.
	EnterGrantOptionFor(c *GrantOptionForContext)

	// EnterExplainOption is called when entering the explainOption production.
	EnterExplainOption(c *ExplainOptionContext)

	// EnterLoadStatement is called when entering the loadStatement production.
	EnterLoadStatement(c *LoadStatementContext)

	// EnterReplicationClause is called when entering the replicationClause production.
	EnterReplicationClause(c *ReplicationClauseContext)

	// EnterExportStatement is called when entering the exportStatement production.
	EnterExportStatement(c *ExportStatementContext)

	// EnterImportStatement is called when entering the importStatement production.
	EnterImportStatement(c *ImportStatementContext)

	// EnterReadStatement is called when entering the readStatement production.
	EnterReadStatement(c *ReadStatementContext)

	// EnterUndoStatement is called when entering the undoStatement production.
	EnterUndoStatement(c *UndoStatementContext)

	// EnterRedoStatement is called when entering the redoStatement production.
	EnterRedoStatement(c *RedoStatementContext)

	// EnterPurgeStatement is called when entering the purgeStatement production.
	EnterPurgeStatement(c *PurgeStatementContext)

	// EnterDropTableVairableStatement is called when entering the dropTableVairableStatement production.
	EnterDropTableVairableStatement(c *DropTableVairableStatementContext)

	// EnterMsckRepairTableStatement is called when entering the msckRepairTableStatement production.
	EnterMsckRepairTableStatement(c *MsckRepairTableStatementContext)

	// EnterDdlStatement is called when entering the ddlStatement production.
	EnterDdlStatement(c *DdlStatementContext)

	// EnterPartitionSpecOrPartitionId is called when entering the partitionSpecOrPartitionId production.
	EnterPartitionSpecOrPartitionId(c *PartitionSpecOrPartitionIdContext)

	// EnterTableOrTableId is called when entering the tableOrTableId production.
	EnterTableOrTableId(c *TableOrTableIdContext)

	// EnterTableHistoryStatement is called when entering the tableHistoryStatement production.
	EnterTableHistoryStatement(c *TableHistoryStatementContext)

	// EnterSetExstore is called when entering the setExstore production.
	EnterSetExstore(c *SetExstoreContext)

	// EnterIfExists is called when entering the ifExists production.
	EnterIfExists(c *IfExistsContext)

	// EnterRestrictOrCascade is called when entering the restrictOrCascade production.
	EnterRestrictOrCascade(c *RestrictOrCascadeContext)

	// EnterIfNotExists is called when entering the ifNotExists production.
	EnterIfNotExists(c *IfNotExistsContext)

	// EnterRewriteEnabled is called when entering the rewriteEnabled production.
	EnterRewriteEnabled(c *RewriteEnabledContext)

	// EnterRewriteDisabled is called when entering the rewriteDisabled production.
	EnterRewriteDisabled(c *RewriteDisabledContext)

	// EnterStoredAsDirs is called when entering the storedAsDirs production.
	EnterStoredAsDirs(c *StoredAsDirsContext)

	// EnterOrReplace is called when entering the orReplace production.
	EnterOrReplace(c *OrReplaceContext)

	// EnterIgnoreProtection is called when entering the ignoreProtection production.
	EnterIgnoreProtection(c *IgnoreProtectionContext)

	// EnterCreateDatabaseStatement is called when entering the createDatabaseStatement production.
	EnterCreateDatabaseStatement(c *CreateDatabaseStatementContext)

	// EnterSchemaName is called when entering the schemaName production.
	EnterSchemaName(c *SchemaNameContext)

	// EnterCreateSchemaStatement is called when entering the createSchemaStatement production.
	EnterCreateSchemaStatement(c *CreateSchemaStatementContext)

	// EnterDbLocation is called when entering the dbLocation production.
	EnterDbLocation(c *DbLocationContext)

	// EnterDbProperties is called when entering the dbProperties production.
	EnterDbProperties(c *DbPropertiesContext)

	// EnterDbPropertiesList is called when entering the dbPropertiesList production.
	EnterDbPropertiesList(c *DbPropertiesListContext)

	// EnterSwitchDatabaseStatement is called when entering the switchDatabaseStatement production.
	EnterSwitchDatabaseStatement(c *SwitchDatabaseStatementContext)

	// EnterDropDatabaseStatement is called when entering the dropDatabaseStatement production.
	EnterDropDatabaseStatement(c *DropDatabaseStatementContext)

	// EnterDropSchemaStatement is called when entering the dropSchemaStatement production.
	EnterDropSchemaStatement(c *DropSchemaStatementContext)

	// EnterDatabaseComment is called when entering the databaseComment production.
	EnterDatabaseComment(c *DatabaseCommentContext)

	// EnterDataFormatDesc is called when entering the dataFormatDesc production.
	EnterDataFormatDesc(c *DataFormatDescContext)

	// EnterCreateTableStatement is called when entering the createTableStatement production.
	EnterCreateTableStatement(c *CreateTableStatementContext)

	// EnterTruncateTableStatement is called when entering the truncateTableStatement production.
	EnterTruncateTableStatement(c *TruncateTableStatementContext)

	// EnterCreateIndexStatement is called when entering the createIndexStatement production.
	EnterCreateIndexStatement(c *CreateIndexStatementContext)

	// EnterIndexComment is called when entering the indexComment production.
	EnterIndexComment(c *IndexCommentContext)

	// EnterAutoRebuild is called when entering the autoRebuild production.
	EnterAutoRebuild(c *AutoRebuildContext)

	// EnterIndexTblName is called when entering the indexTblName production.
	EnterIndexTblName(c *IndexTblNameContext)

	// EnterIndexPropertiesPrefixed is called when entering the indexPropertiesPrefixed production.
	EnterIndexPropertiesPrefixed(c *IndexPropertiesPrefixedContext)

	// EnterIndexProperties is called when entering the indexProperties production.
	EnterIndexProperties(c *IndexPropertiesContext)

	// EnterIndexPropertiesList is called when entering the indexPropertiesList production.
	EnterIndexPropertiesList(c *IndexPropertiesListContext)

	// EnterDropIndexStatement is called when entering the dropIndexStatement production.
	EnterDropIndexStatement(c *DropIndexStatementContext)

	// EnterDropTableStatement is called when entering the dropTableStatement production.
	EnterDropTableStatement(c *DropTableStatementContext)

	// EnterAlterStatement is called when entering the alterStatement production.
	EnterAlterStatement(c *AlterStatementContext)

	// EnterAlterSchemaStatementSuffix is called when entering the alterSchemaStatementSuffix production.
	EnterAlterSchemaStatementSuffix(c *AlterSchemaStatementSuffixContext)

	// EnterAlterTableStatementSuffix is called when entering the alterTableStatementSuffix production.
	EnterAlterTableStatementSuffix(c *AlterTableStatementSuffixContext)

	// EnterAlterTableMergePartitionSuffix is called when entering the alterTableMergePartitionSuffix production.
	EnterAlterTableMergePartitionSuffix(c *AlterTableMergePartitionSuffixContext)

	// EnterAlterStatementSuffixAddConstraint is called when entering the alterStatementSuffixAddConstraint production.
	EnterAlterStatementSuffixAddConstraint(c *AlterStatementSuffixAddConstraintContext)

	// EnterAlterTblPartitionStatementSuffix is called when entering the alterTblPartitionStatementSuffix production.
	EnterAlterTblPartitionStatementSuffix(c *AlterTblPartitionStatementSuffixContext)

	// EnterAlterStatementSuffixPartitionLifecycle is called when entering the alterStatementSuffixPartitionLifecycle production.
	EnterAlterStatementSuffixPartitionLifecycle(c *AlterStatementSuffixPartitionLifecycleContext)

	// EnterAlterTblPartitionStatementSuffixProperties is called when entering the alterTblPartitionStatementSuffixProperties production.
	EnterAlterTblPartitionStatementSuffixProperties(c *AlterTblPartitionStatementSuffixPropertiesContext)

	// EnterAlterStatementPartitionKeyType is called when entering the alterStatementPartitionKeyType production.
	EnterAlterStatementPartitionKeyType(c *AlterStatementPartitionKeyTypeContext)

	// EnterAlterViewStatementSuffix is called when entering the alterViewStatementSuffix production.
	EnterAlterViewStatementSuffix(c *AlterViewStatementSuffixContext)

	// EnterAlterMaterializedViewStatementSuffix is called when entering the alterMaterializedViewStatementSuffix production.
	EnterAlterMaterializedViewStatementSuffix(c *AlterMaterializedViewStatementSuffixContext)

	// EnterAlterMaterializedViewSuffixRewrite is called when entering the alterMaterializedViewSuffixRewrite production.
	EnterAlterMaterializedViewSuffixRewrite(c *AlterMaterializedViewSuffixRewriteContext)

	// EnterAlterMaterializedViewSuffixRebuild is called when entering the alterMaterializedViewSuffixRebuild production.
	EnterAlterMaterializedViewSuffixRebuild(c *AlterMaterializedViewSuffixRebuildContext)

	// EnterAlterIndexStatementSuffix is called when entering the alterIndexStatementSuffix production.
	EnterAlterIndexStatementSuffix(c *AlterIndexStatementSuffixContext)

	// EnterAlterDatabaseStatementSuffix is called when entering the alterDatabaseStatementSuffix production.
	EnterAlterDatabaseStatementSuffix(c *AlterDatabaseStatementSuffixContext)

	// EnterAlterDatabaseSuffixProperties is called when entering the alterDatabaseSuffixProperties production.
	EnterAlterDatabaseSuffixProperties(c *AlterDatabaseSuffixPropertiesContext)

	// EnterAlterDatabaseSuffixSetOwner is called when entering the alterDatabaseSuffixSetOwner production.
	EnterAlterDatabaseSuffixSetOwner(c *AlterDatabaseSuffixSetOwnerContext)

	// EnterAlterStatementSuffixRename is called when entering the alterStatementSuffixRename production.
	EnterAlterStatementSuffixRename(c *AlterStatementSuffixRenameContext)

	// EnterAlterStatementSuffixAddCol is called when entering the alterStatementSuffixAddCol production.
	EnterAlterStatementSuffixAddCol(c *AlterStatementSuffixAddColContext)

	// EnterAlterStatementSuffixRenameCol is called when entering the alterStatementSuffixRenameCol production.
	EnterAlterStatementSuffixRenameCol(c *AlterStatementSuffixRenameColContext)

	// EnterAlterStatementSuffixDropCol is called when entering the alterStatementSuffixDropCol production.
	EnterAlterStatementSuffixDropCol(c *AlterStatementSuffixDropColContext)

	// EnterAlterStatementSuffixUpdateStatsCol is called when entering the alterStatementSuffixUpdateStatsCol production.
	EnterAlterStatementSuffixUpdateStatsCol(c *AlterStatementSuffixUpdateStatsColContext)

	// EnterAlterStatementChangeColPosition is called when entering the alterStatementChangeColPosition production.
	EnterAlterStatementChangeColPosition(c *AlterStatementChangeColPositionContext)

	// EnterAlterStatementSuffixAddPartitions is called when entering the alterStatementSuffixAddPartitions production.
	EnterAlterStatementSuffixAddPartitions(c *AlterStatementSuffixAddPartitionsContext)

	// EnterAlterStatementSuffixAddPartitionsElement is called when entering the alterStatementSuffixAddPartitionsElement production.
	EnterAlterStatementSuffixAddPartitionsElement(c *AlterStatementSuffixAddPartitionsElementContext)

	// EnterAlterStatementSuffixTouch is called when entering the alterStatementSuffixTouch production.
	EnterAlterStatementSuffixTouch(c *AlterStatementSuffixTouchContext)

	// EnterAlterStatementSuffixArchive is called when entering the alterStatementSuffixArchive production.
	EnterAlterStatementSuffixArchive(c *AlterStatementSuffixArchiveContext)

	// EnterAlterStatementSuffixUnArchive is called when entering the alterStatementSuffixUnArchive production.
	EnterAlterStatementSuffixUnArchive(c *AlterStatementSuffixUnArchiveContext)

	// EnterAlterStatementSuffixChangeOwner is called when entering the alterStatementSuffixChangeOwner production.
	EnterAlterStatementSuffixChangeOwner(c *AlterStatementSuffixChangeOwnerContext)

	// EnterPartitionLocation is called when entering the partitionLocation production.
	EnterPartitionLocation(c *PartitionLocationContext)

	// EnterAlterStatementSuffixDropPartitions is called when entering the alterStatementSuffixDropPartitions production.
	EnterAlterStatementSuffixDropPartitions(c *AlterStatementSuffixDropPartitionsContext)

	// EnterAlterStatementSuffixProperties is called when entering the alterStatementSuffixProperties production.
	EnterAlterStatementSuffixProperties(c *AlterStatementSuffixPropertiesContext)

	// EnterAlterViewSuffixProperties is called when entering the alterViewSuffixProperties production.
	EnterAlterViewSuffixProperties(c *AlterViewSuffixPropertiesContext)

	// EnterAlterViewColumnCommentSuffix is called when entering the alterViewColumnCommentSuffix production.
	EnterAlterViewColumnCommentSuffix(c *AlterViewColumnCommentSuffixContext)

	// EnterAlterStatementSuffixSerdeProperties is called when entering the alterStatementSuffixSerdeProperties production.
	EnterAlterStatementSuffixSerdeProperties(c *AlterStatementSuffixSerdePropertiesContext)

	// EnterTablePartitionPrefix is called when entering the tablePartitionPrefix production.
	EnterTablePartitionPrefix(c *TablePartitionPrefixContext)

	// EnterAlterStatementSuffixFileFormat is called when entering the alterStatementSuffixFileFormat production.
	EnterAlterStatementSuffixFileFormat(c *AlterStatementSuffixFileFormatContext)

	// EnterAlterStatementSuffixClusterbySortby is called when entering the alterStatementSuffixClusterbySortby production.
	EnterAlterStatementSuffixClusterbySortby(c *AlterStatementSuffixClusterbySortbyContext)

	// EnterAlterTblPartitionStatementSuffixSkewedLocation is called when entering the alterTblPartitionStatementSuffixSkewedLocation production.
	EnterAlterTblPartitionStatementSuffixSkewedLocation(c *AlterTblPartitionStatementSuffixSkewedLocationContext)

	// EnterSkewedLocations is called when entering the skewedLocations production.
	EnterSkewedLocations(c *SkewedLocationsContext)

	// EnterSkewedLocationsList is called when entering the skewedLocationsList production.
	EnterSkewedLocationsList(c *SkewedLocationsListContext)

	// EnterSkewedLocationMap is called when entering the skewedLocationMap production.
	EnterSkewedLocationMap(c *SkewedLocationMapContext)

	// EnterAlterStatementSuffixLocation is called when entering the alterStatementSuffixLocation production.
	EnterAlterStatementSuffixLocation(c *AlterStatementSuffixLocationContext)

	// EnterAlterStatementSuffixSkewedby is called when entering the alterStatementSuffixSkewedby production.
	EnterAlterStatementSuffixSkewedby(c *AlterStatementSuffixSkewedbyContext)

	// EnterAlterStatementSuffixExchangePartition is called when entering the alterStatementSuffixExchangePartition production.
	EnterAlterStatementSuffixExchangePartition(c *AlterStatementSuffixExchangePartitionContext)

	// EnterAlterStatementSuffixProtectMode is called when entering the alterStatementSuffixProtectMode production.
	EnterAlterStatementSuffixProtectMode(c *AlterStatementSuffixProtectModeContext)

	// EnterAlterStatementSuffixRenamePart is called when entering the alterStatementSuffixRenamePart production.
	EnterAlterStatementSuffixRenamePart(c *AlterStatementSuffixRenamePartContext)

	// EnterAlterStatementSuffixStatsPart is called when entering the alterStatementSuffixStatsPart production.
	EnterAlterStatementSuffixStatsPart(c *AlterStatementSuffixStatsPartContext)

	// EnterAlterStatementSuffixMergeFiles is called when entering the alterStatementSuffixMergeFiles production.
	EnterAlterStatementSuffixMergeFiles(c *AlterStatementSuffixMergeFilesContext)

	// EnterAlterProtectMode is called when entering the alterProtectMode production.
	EnterAlterProtectMode(c *AlterProtectModeContext)

	// EnterAlterProtectModeMode is called when entering the alterProtectModeMode production.
	EnterAlterProtectModeMode(c *AlterProtectModeModeContext)

	// EnterAlterStatementSuffixBucketNum is called when entering the alterStatementSuffixBucketNum production.
	EnterAlterStatementSuffixBucketNum(c *AlterStatementSuffixBucketNumContext)

	// EnterAlterStatementSuffixCompact is called when entering the alterStatementSuffixCompact production.
	EnterAlterStatementSuffixCompact(c *AlterStatementSuffixCompactContext)

	// EnterFileFormat is called when entering the fileFormat production.
	EnterFileFormat(c *FileFormatContext)

	// EnterTabTypeExpr is called when entering the tabTypeExpr production.
	EnterTabTypeExpr(c *TabTypeExprContext)

	// EnterPartTypeExpr is called when entering the partTypeExpr production.
	EnterPartTypeExpr(c *PartTypeExprContext)

	// EnterDescStatement is called when entering the descStatement production.
	EnterDescStatement(c *DescStatementContext)

	// EnterAnalyzeStatement is called when entering the analyzeStatement production.
	EnterAnalyzeStatement(c *AnalyzeStatementContext)

	// EnterForColumnsStatement is called when entering the forColumnsStatement production.
	EnterForColumnsStatement(c *ForColumnsStatementContext)

	// EnterColumnNameOrList is called when entering the columnNameOrList production.
	EnterColumnNameOrList(c *ColumnNameOrListContext)

	// EnterShowStatement is called when entering the showStatement production.
	EnterShowStatement(c *ShowStatementContext)

	// EnterListStatement is called when entering the listStatement production.
	EnterListStatement(c *ListStatementContext)

	// EnterBareDate is called when entering the bareDate production.
	EnterBareDate(c *BareDateContext)

	// EnterLockStatement is called when entering the lockStatement production.
	EnterLockStatement(c *LockStatementContext)

	// EnterLockDatabase is called when entering the lockDatabase production.
	EnterLockDatabase(c *LockDatabaseContext)

	// EnterLockMode is called when entering the lockMode production.
	EnterLockMode(c *LockModeContext)

	// EnterUnlockStatement is called when entering the unlockStatement production.
	EnterUnlockStatement(c *UnlockStatementContext)

	// EnterUnlockDatabase is called when entering the unlockDatabase production.
	EnterUnlockDatabase(c *UnlockDatabaseContext)

	// EnterResourceList is called when entering the resourceList production.
	EnterResourceList(c *ResourceListContext)

	// EnterResource is called when entering the resource production.
	EnterResource(c *ResourceContext)

	// EnterResourceType is called when entering the resourceType production.
	EnterResourceType(c *ResourceTypeContext)

	// EnterCreateFunctionStatement is called when entering the createFunctionStatement production.
	EnterCreateFunctionStatement(c *CreateFunctionStatementContext)

	// EnterDropFunctionStatement is called when entering the dropFunctionStatement production.
	EnterDropFunctionStatement(c *DropFunctionStatementContext)

	// EnterReloadFunctionStatement is called when entering the reloadFunctionStatement production.
	EnterReloadFunctionStatement(c *ReloadFunctionStatementContext)

	// EnterCreateMacroStatement is called when entering the createMacroStatement production.
	EnterCreateMacroStatement(c *CreateMacroStatementContext)

	// EnterDropMacroStatement is called when entering the dropMacroStatement production.
	EnterDropMacroStatement(c *DropMacroStatementContext)

	// EnterCreateSqlFunctionStatement is called when entering the createSqlFunctionStatement production.
	EnterCreateSqlFunctionStatement(c *CreateSqlFunctionStatementContext)

	// EnterCloneTableStatement is called when entering the cloneTableStatement production.
	EnterCloneTableStatement(c *CloneTableStatementContext)

	// EnterCreateViewStatement is called when entering the createViewStatement production.
	EnterCreateViewStatement(c *CreateViewStatementContext)

	// EnterViewPartition is called when entering the viewPartition production.
	EnterViewPartition(c *ViewPartitionContext)

	// EnterDropViewStatement is called when entering the dropViewStatement production.
	EnterDropViewStatement(c *DropViewStatementContext)

	// EnterCreateMaterializedViewStatement is called when entering the createMaterializedViewStatement production.
	EnterCreateMaterializedViewStatement(c *CreateMaterializedViewStatementContext)

	// EnterDropMaterializedViewStatement is called when entering the dropMaterializedViewStatement production.
	EnterDropMaterializedViewStatement(c *DropMaterializedViewStatementContext)

	// EnterShowFunctionIdentifier is called when entering the showFunctionIdentifier production.
	EnterShowFunctionIdentifier(c *ShowFunctionIdentifierContext)

	// EnterShowStmtIdentifier is called when entering the showStmtIdentifier production.
	EnterShowStmtIdentifier(c *ShowStmtIdentifierContext)

	// EnterTableComment is called when entering the tableComment production.
	EnterTableComment(c *TableCommentContext)

	// EnterTablePartition is called when entering the tablePartition production.
	EnterTablePartition(c *TablePartitionContext)

	// EnterTableBuckets is called when entering the tableBuckets production.
	EnterTableBuckets(c *TableBucketsContext)

	// EnterTableShards is called when entering the tableShards production.
	EnterTableShards(c *TableShardsContext)

	// EnterTableSkewed is called when entering the tableSkewed production.
	EnterTableSkewed(c *TableSkewedContext)

	// EnterRowFormat is called when entering the rowFormat production.
	EnterRowFormat(c *RowFormatContext)

	// EnterRecordReader is called when entering the recordReader production.
	EnterRecordReader(c *RecordReaderContext)

	// EnterRecordWriter is called when entering the recordWriter production.
	EnterRecordWriter(c *RecordWriterContext)

	// EnterRowFormatSerde is called when entering the rowFormatSerde production.
	EnterRowFormatSerde(c *RowFormatSerdeContext)

	// EnterRowFormatDelimited is called when entering the rowFormatDelimited production.
	EnterRowFormatDelimited(c *RowFormatDelimitedContext)

	// EnterTableRowFormat is called when entering the tableRowFormat production.
	EnterTableRowFormat(c *TableRowFormatContext)

	// EnterTablePropertiesPrefixed is called when entering the tablePropertiesPrefixed production.
	EnterTablePropertiesPrefixed(c *TablePropertiesPrefixedContext)

	// EnterTableProperties is called when entering the tableProperties production.
	EnterTableProperties(c *TablePropertiesContext)

	// EnterTablePropertiesList is called when entering the tablePropertiesList production.
	EnterTablePropertiesList(c *TablePropertiesListContext)

	// EnterKeyValueProperty is called when entering the keyValueProperty production.
	EnterKeyValueProperty(c *KeyValuePropertyContext)

	// EnterUserDefinedJoinPropertiesList is called when entering the userDefinedJoinPropertiesList production.
	EnterUserDefinedJoinPropertiesList(c *UserDefinedJoinPropertiesListContext)

	// EnterKeyPrivProperty is called when entering the keyPrivProperty production.
	EnterKeyPrivProperty(c *KeyPrivPropertyContext)

	// EnterKeyProperty is called when entering the keyProperty production.
	EnterKeyProperty(c *KeyPropertyContext)

	// EnterTableRowFormatFieldIdentifier is called when entering the tableRowFormatFieldIdentifier production.
	EnterTableRowFormatFieldIdentifier(c *TableRowFormatFieldIdentifierContext)

	// EnterTableRowFormatCollItemsIdentifier is called when entering the tableRowFormatCollItemsIdentifier production.
	EnterTableRowFormatCollItemsIdentifier(c *TableRowFormatCollItemsIdentifierContext)

	// EnterTableRowFormatMapKeysIdentifier is called when entering the tableRowFormatMapKeysIdentifier production.
	EnterTableRowFormatMapKeysIdentifier(c *TableRowFormatMapKeysIdentifierContext)

	// EnterTableRowFormatLinesIdentifier is called when entering the tableRowFormatLinesIdentifier production.
	EnterTableRowFormatLinesIdentifier(c *TableRowFormatLinesIdentifierContext)

	// EnterTableRowNullFormat is called when entering the tableRowNullFormat production.
	EnterTableRowNullFormat(c *TableRowNullFormatContext)

	// EnterTableFileFormat is called when entering the tableFileFormat production.
	EnterTableFileFormat(c *TableFileFormatContext)

	// EnterTableLocation is called when entering the tableLocation production.
	EnterTableLocation(c *TableLocationContext)

	// EnterExternalTableResource is called when entering the externalTableResource production.
	EnterExternalTableResource(c *ExternalTableResourceContext)

	// EnterViewResource is called when entering the viewResource production.
	EnterViewResource(c *ViewResourceContext)

	// EnterOutOfLineConstraints is called when entering the outOfLineConstraints production.
	EnterOutOfLineConstraints(c *OutOfLineConstraintsContext)

	// EnterEnableSpec is called when entering the enableSpec production.
	EnterEnableSpec(c *EnableSpecContext)

	// EnterValidateSpec is called when entering the validateSpec production.
	EnterValidateSpec(c *ValidateSpecContext)

	// EnterRelySpec is called when entering the relySpec production.
	EnterRelySpec(c *RelySpecContext)

	// EnterColumnNameTypeConstraintList is called when entering the columnNameTypeConstraintList production.
	EnterColumnNameTypeConstraintList(c *ColumnNameTypeConstraintListContext)

	// EnterColumnNameTypeList is called when entering the columnNameTypeList production.
	EnterColumnNameTypeList(c *ColumnNameTypeListContext)

	// EnterPartitionColumnNameTypeList is called when entering the partitionColumnNameTypeList production.
	EnterPartitionColumnNameTypeList(c *PartitionColumnNameTypeListContext)

	// EnterColumnNameTypeConstraintWithPosList is called when entering the columnNameTypeConstraintWithPosList production.
	EnterColumnNameTypeConstraintWithPosList(c *ColumnNameTypeConstraintWithPosListContext)

	// EnterColumnNameColonTypeList is called when entering the columnNameColonTypeList production.
	EnterColumnNameColonTypeList(c *ColumnNameColonTypeListContext)

	// EnterColumnNameList is called when entering the columnNameList production.
	EnterColumnNameList(c *ColumnNameListContext)

	// EnterColumnNameListInParentheses is called when entering the columnNameListInParentheses production.
	EnterColumnNameListInParentheses(c *ColumnNameListInParenthesesContext)

	// EnterColumnName is called when entering the columnName production.
	EnterColumnName(c *ColumnNameContext)

	// EnterColumnNameOrderList is called when entering the columnNameOrderList production.
	EnterColumnNameOrderList(c *ColumnNameOrderListContext)

	// EnterClusterColumnNameOrderList is called when entering the clusterColumnNameOrderList production.
	EnterClusterColumnNameOrderList(c *ClusterColumnNameOrderListContext)

	// EnterSkewedValueElement is called when entering the skewedValueElement production.
	EnterSkewedValueElement(c *SkewedValueElementContext)

	// EnterSkewedColumnValuePairList is called when entering the skewedColumnValuePairList production.
	EnterSkewedColumnValuePairList(c *SkewedColumnValuePairListContext)

	// EnterSkewedColumnValuePair is called when entering the skewedColumnValuePair production.
	EnterSkewedColumnValuePair(c *SkewedColumnValuePairContext)

	// EnterSkewedColumnValues is called when entering the skewedColumnValues production.
	EnterSkewedColumnValues(c *SkewedColumnValuesContext)

	// EnterSkewedColumnValue is called when entering the skewedColumnValue production.
	EnterSkewedColumnValue(c *SkewedColumnValueContext)

	// EnterSkewedValueLocationElement is called when entering the skewedValueLocationElement production.
	EnterSkewedValueLocationElement(c *SkewedValueLocationElementContext)

	// EnterColumnNameOrder is called when entering the columnNameOrder production.
	EnterColumnNameOrder(c *ColumnNameOrderContext)

	// EnterColumnNameCommentList is called when entering the columnNameCommentList production.
	EnterColumnNameCommentList(c *ColumnNameCommentListContext)

	// EnterColumnNameComment is called when entering the columnNameComment production.
	EnterColumnNameComment(c *ColumnNameCommentContext)

	// EnterColumnRefOrder is called when entering the columnRefOrder production.
	EnterColumnRefOrder(c *ColumnRefOrderContext)

	// EnterColumnNameTypeConstraint is called when entering the columnNameTypeConstraint production.
	EnterColumnNameTypeConstraint(c *ColumnNameTypeConstraintContext)

	// EnterColumnNameType is called when entering the columnNameType production.
	EnterColumnNameType(c *ColumnNameTypeContext)

	// EnterPartitionColumnNameType is called when entering the partitionColumnNameType production.
	EnterPartitionColumnNameType(c *PartitionColumnNameTypeContext)

	// EnterMultipartIdentifier is called when entering the multipartIdentifier production.
	EnterMultipartIdentifier(c *MultipartIdentifierContext)

	// EnterColumnNameTypeConstraintWithPos is called when entering the columnNameTypeConstraintWithPos production.
	EnterColumnNameTypeConstraintWithPos(c *ColumnNameTypeConstraintWithPosContext)

	// EnterConstraints is called when entering the constraints production.
	EnterConstraints(c *ConstraintsContext)

	// EnterPrimaryKey is called when entering the primaryKey production.
	EnterPrimaryKey(c *PrimaryKeyContext)

	// EnterNullableSpec is called when entering the nullableSpec production.
	EnterNullableSpec(c *NullableSpecContext)

	// EnterDefaultValue is called when entering the defaultValue production.
	EnterDefaultValue(c *DefaultValueContext)

	// EnterColumnNameColonType is called when entering the columnNameColonType production.
	EnterColumnNameColonType(c *ColumnNameColonTypeContext)

	// EnterColType is called when entering the colType production.
	EnterColType(c *ColTypeContext)

	// EnterColTypeList is called when entering the colTypeList production.
	EnterColTypeList(c *ColTypeListContext)

	// EnterAnyType is called when entering the anyType production.
	EnterAnyType(c *AnyTypeContext)

	// EnterAnyTypeList is called when entering the anyTypeList production.
	EnterAnyTypeList(c *AnyTypeListContext)

	// EnterTableTypeInfo is called when entering the tableTypeInfo production.
	EnterTableTypeInfo(c *TableTypeInfoContext)

	// EnterType is called when entering the type production.
	EnterType(c *TypeContext)

	// EnterPrimitiveType is called when entering the primitiveType production.
	EnterPrimitiveType(c *PrimitiveTypeContext)

	// EnterBuiltinTypeOrUdt is called when entering the builtinTypeOrUdt production.
	EnterBuiltinTypeOrUdt(c *BuiltinTypeOrUdtContext)

	// EnterPrimitiveTypeOrUdt is called when entering the primitiveTypeOrUdt production.
	EnterPrimitiveTypeOrUdt(c *PrimitiveTypeOrUdtContext)

	// EnterListType is called when entering the listType production.
	EnterListType(c *ListTypeContext)

	// EnterStructType is called when entering the structType production.
	EnterStructType(c *StructTypeContext)

	// EnterMapType is called when entering the mapType production.
	EnterMapType(c *MapTypeContext)

	// EnterUnionType is called when entering the unionType production.
	EnterUnionType(c *UnionTypeContext)

	// EnterSetOperator is called when entering the setOperator production.
	EnterSetOperator(c *SetOperatorContext)

	// EnterWithClause is called when entering the withClause production.
	EnterWithClause(c *WithClauseContext)

	// EnterInsertClause is called when entering the insertClause production.
	EnterInsertClause(c *InsertClauseContext)

	// EnterDestination is called when entering the destination production.
	EnterDestination(c *DestinationContext)

	// EnterDeleteStatement is called when entering the deleteStatement production.
	EnterDeleteStatement(c *DeleteStatementContext)

	// EnterColumnAssignmentClause is called when entering the columnAssignmentClause production.
	EnterColumnAssignmentClause(c *ColumnAssignmentClauseContext)

	// EnterSetColumnsClause is called when entering the setColumnsClause production.
	EnterSetColumnsClause(c *SetColumnsClauseContext)

	// EnterUpdateStatement is called when entering the updateStatement production.
	EnterUpdateStatement(c *UpdateStatementContext)

	// EnterMergeStatement is called when entering the mergeStatement production.
	EnterMergeStatement(c *MergeStatementContext)

	// EnterMergeTargetTable is called when entering the mergeTargetTable production.
	EnterMergeTargetTable(c *MergeTargetTableContext)

	// EnterMergeSourceTable is called when entering the mergeSourceTable production.
	EnterMergeSourceTable(c *MergeSourceTableContext)

	// EnterMergeAction is called when entering the mergeAction production.
	EnterMergeAction(c *MergeActionContext)

	// EnterMergeValuesCaluse is called when entering the mergeValuesCaluse production.
	EnterMergeValuesCaluse(c *MergeValuesCaluseContext)

	// EnterMergeSetColumnsClause is called when entering the mergeSetColumnsClause production.
	EnterMergeSetColumnsClause(c *MergeSetColumnsClauseContext)

	// EnterMergeColumnAssignmentClause is called when entering the mergeColumnAssignmentClause production.
	EnterMergeColumnAssignmentClause(c *MergeColumnAssignmentClauseContext)

	// EnterSelectClause is called when entering the selectClause production.
	EnterSelectClause(c *SelectClauseContext)

	// EnterSelectList is called when entering the selectList production.
	EnterSelectList(c *SelectListContext)

	// EnterSelectTrfmClause is called when entering the selectTrfmClause production.
	EnterSelectTrfmClause(c *SelectTrfmClauseContext)

	// EnterHintClause is called when entering the hintClause production.
	EnterHintClause(c *HintClauseContext)

	// EnterHintList is called when entering the hintList production.
	EnterHintList(c *HintListContext)

	// EnterHintItem is called when entering the hintItem production.
	EnterHintItem(c *HintItemContext)

	// EnterDynamicfilterHint is called when entering the dynamicfilterHint production.
	EnterDynamicfilterHint(c *DynamicfilterHintContext)

	// EnterMapJoinHint is called when entering the mapJoinHint production.
	EnterMapJoinHint(c *MapJoinHintContext)

	// EnterSkewJoinHint is called when entering the skewJoinHint production.
	EnterSkewJoinHint(c *SkewJoinHintContext)

	// EnterSelectivityHint is called when entering the selectivityHint production.
	EnterSelectivityHint(c *SelectivityHintContext)

	// EnterMultipleSkewHintArgs is called when entering the multipleSkewHintArgs production.
	EnterMultipleSkewHintArgs(c *MultipleSkewHintArgsContext)

	// EnterSkewJoinHintArgs is called when entering the skewJoinHintArgs production.
	EnterSkewJoinHintArgs(c *SkewJoinHintArgsContext)

	// EnterSkewColumns is called when entering the skewColumns production.
	EnterSkewColumns(c *SkewColumnsContext)

	// EnterSkewJoinHintKeyValues is called when entering the skewJoinHintKeyValues production.
	EnterSkewJoinHintKeyValues(c *SkewJoinHintKeyValuesContext)

	// EnterHintName is called when entering the hintName production.
	EnterHintName(c *HintNameContext)

	// EnterHintArgs is called when entering the hintArgs production.
	EnterHintArgs(c *HintArgsContext)

	// EnterHintArgName is called when entering the hintArgName production.
	EnterHintArgName(c *HintArgNameContext)

	// EnterSelectItem is called when entering the selectItem production.
	EnterSelectItem(c *SelectItemContext)

	// EnterTrfmClause is called when entering the trfmClause production.
	EnterTrfmClause(c *TrfmClauseContext)

	// EnterSelectExpression is called when entering the selectExpression production.
	EnterSelectExpression(c *SelectExpressionContext)

	// EnterSelectExpressionList is called when entering the selectExpressionList production.
	EnterSelectExpressionList(c *SelectExpressionListContext)

	// EnterWindow_clause is called when entering the window_clause production.
	EnterWindow_clause(c *Window_clauseContext)

	// EnterWindow_defn is called when entering the window_defn production.
	EnterWindow_defn(c *Window_defnContext)

	// EnterWindow_specification is called when entering the window_specification production.
	EnterWindow_specification(c *Window_specificationContext)

	// EnterWindow_frame is called when entering the window_frame production.
	EnterWindow_frame(c *Window_frameContext)

	// EnterFrame_exclusion is called when entering the frame_exclusion production.
	EnterFrame_exclusion(c *Frame_exclusionContext)

	// EnterWindow_frame_start_boundary is called when entering the window_frame_start_boundary production.
	EnterWindow_frame_start_boundary(c *Window_frame_start_boundaryContext)

	// EnterWindow_frame_boundary is called when entering the window_frame_boundary production.
	EnterWindow_frame_boundary(c *Window_frame_boundaryContext)

	// EnterTableAllColumns is called when entering the tableAllColumns production.
	EnterTableAllColumns(c *TableAllColumnsContext)

	// EnterTableOrColumn is called when entering the tableOrColumn production.
	EnterTableOrColumn(c *TableOrColumnContext)

	// EnterTableAndColumnRef is called when entering the tableAndColumnRef production.
	EnterTableAndColumnRef(c *TableAndColumnRefContext)

	// EnterExpressionList is called when entering the expressionList production.
	EnterExpressionList(c *ExpressionListContext)

	// EnterAliasList is called when entering the aliasList production.
	EnterAliasList(c *AliasListContext)

	// EnterFromClause is called when entering the fromClause production.
	EnterFromClause(c *FromClauseContext)

	// EnterJoinSource is called when entering the joinSource production.
	EnterJoinSource(c *JoinSourceContext)

	// EnterJoinRHS is called when entering the joinRHS production.
	EnterJoinRHS(c *JoinRHSContext)

	// EnterUniqueJoinSource is called when entering the uniqueJoinSource production.
	EnterUniqueJoinSource(c *UniqueJoinSourceContext)

	// EnterUniqueJoinExpr is called when entering the uniqueJoinExpr production.
	EnterUniqueJoinExpr(c *UniqueJoinExprContext)

	// EnterUniqueJoinToken is called when entering the uniqueJoinToken production.
	EnterUniqueJoinToken(c *UniqueJoinTokenContext)

	// EnterJoinToken is called when entering the joinToken production.
	EnterJoinToken(c *JoinTokenContext)

	// EnterLateralView is called when entering the lateralView production.
	EnterLateralView(c *LateralViewContext)

	// EnterTableAlias is called when entering the tableAlias production.
	EnterTableAlias(c *TableAliasContext)

	// EnterTableBucketSample is called when entering the tableBucketSample production.
	EnterTableBucketSample(c *TableBucketSampleContext)

	// EnterSplitSample is called when entering the splitSample production.
	EnterSplitSample(c *SplitSampleContext)

	// EnterTableSample is called when entering the tableSample production.
	EnterTableSample(c *TableSampleContext)

	// EnterTableSource is called when entering the tableSource production.
	EnterTableSource(c *TableSourceContext)

	// EnterAvailableSql11KeywordsForOdpsTableAlias is called when entering the availableSql11KeywordsForOdpsTableAlias production.
	EnterAvailableSql11KeywordsForOdpsTableAlias(c *AvailableSql11KeywordsForOdpsTableAliasContext)

	// EnterTableName is called when entering the tableName production.
	EnterTableName(c *TableNameContext)

	// EnterPartitioningSpec is called when entering the partitioningSpec production.
	EnterPartitioningSpec(c *PartitioningSpecContext)

	// EnterPartitionTableFunctionSource is called when entering the partitionTableFunctionSource production.
	EnterPartitionTableFunctionSource(c *PartitionTableFunctionSourceContext)

	// EnterPartitionedTableFunction is called when entering the partitionedTableFunction production.
	EnterPartitionedTableFunction(c *PartitionedTableFunctionContext)

	// EnterWhereClause is called when entering the whereClause production.
	EnterWhereClause(c *WhereClauseContext)

	// EnterValueRowConstructor is called when entering the valueRowConstructor production.
	EnterValueRowConstructor(c *ValueRowConstructorContext)

	// EnterValuesTableConstructor is called when entering the valuesTableConstructor production.
	EnterValuesTableConstructor(c *ValuesTableConstructorContext)

	// EnterValuesClause is called when entering the valuesClause production.
	EnterValuesClause(c *ValuesClauseContext)

	// EnterVirtualTableSource is called when entering the virtualTableSource production.
	EnterVirtualTableSource(c *VirtualTableSourceContext)

	// EnterTableNameColList is called when entering the tableNameColList production.
	EnterTableNameColList(c *TableNameColListContext)

	// EnterFunctionTypeCubeOrRollup is called when entering the functionTypeCubeOrRollup production.
	EnterFunctionTypeCubeOrRollup(c *FunctionTypeCubeOrRollupContext)

	// EnterGroupingSetsItem is called when entering the groupingSetsItem production.
	EnterGroupingSetsItem(c *GroupingSetsItemContext)

	// EnterGroupingSetsClause is called when entering the groupingSetsClause production.
	EnterGroupingSetsClause(c *GroupingSetsClauseContext)

	// EnterGroupByKey is called when entering the groupByKey production.
	EnterGroupByKey(c *GroupByKeyContext)

	// EnterGroupByClause is called when entering the groupByClause production.
	EnterGroupByClause(c *GroupByClauseContext)

	// EnterGroupingSetExpression is called when entering the groupingSetExpression production.
	EnterGroupingSetExpression(c *GroupingSetExpressionContext)

	// EnterGroupingSetExpressionMultiple is called when entering the groupingSetExpressionMultiple production.
	EnterGroupingSetExpressionMultiple(c *GroupingSetExpressionMultipleContext)

	// EnterGroupingExpressionSingle is called when entering the groupingExpressionSingle production.
	EnterGroupingExpressionSingle(c *GroupingExpressionSingleContext)

	// EnterHavingClause is called when entering the havingClause production.
	EnterHavingClause(c *HavingClauseContext)

	// EnterHavingCondition is called when entering the havingCondition production.
	EnterHavingCondition(c *HavingConditionContext)

	// EnterExpressionsInParenthese is called when entering the expressionsInParenthese production.
	EnterExpressionsInParenthese(c *ExpressionsInParentheseContext)

	// EnterExpressionsNotInParenthese is called when entering the expressionsNotInParenthese production.
	EnterExpressionsNotInParenthese(c *ExpressionsNotInParentheseContext)

	// EnterColumnRefOrderInParenthese is called when entering the columnRefOrderInParenthese production.
	EnterColumnRefOrderInParenthese(c *ColumnRefOrderInParentheseContext)

	// EnterColumnRefOrderNotInParenthese is called when entering the columnRefOrderNotInParenthese production.
	EnterColumnRefOrderNotInParenthese(c *ColumnRefOrderNotInParentheseContext)

	// EnterOrderByClause is called when entering the orderByClause production.
	EnterOrderByClause(c *OrderByClauseContext)

	// EnterColumnNameOrIndexInParenthese is called when entering the columnNameOrIndexInParenthese production.
	EnterColumnNameOrIndexInParenthese(c *ColumnNameOrIndexInParentheseContext)

	// EnterColumnNameOrIndexNotInParenthese is called when entering the columnNameOrIndexNotInParenthese production.
	EnterColumnNameOrIndexNotInParenthese(c *ColumnNameOrIndexNotInParentheseContext)

	// EnterColumnNameOrIndex is called when entering the columnNameOrIndex production.
	EnterColumnNameOrIndex(c *ColumnNameOrIndexContext)

	// EnterZorderByClause is called when entering the zorderByClause production.
	EnterZorderByClause(c *ZorderByClauseContext)

	// EnterClusterByClause is called when entering the clusterByClause production.
	EnterClusterByClause(c *ClusterByClauseContext)

	// EnterPartitionByClause is called when entering the partitionByClause production.
	EnterPartitionByClause(c *PartitionByClauseContext)

	// EnterDistributeByClause is called when entering the distributeByClause production.
	EnterDistributeByClause(c *DistributeByClauseContext)

	// EnterSortByClause is called when entering the sortByClause production.
	EnterSortByClause(c *SortByClauseContext)

	// EnterFunction is called when entering the function production.
	EnterFunction(c *FunctionContext)

	// EnterFunctionArgument is called when entering the functionArgument production.
	EnterFunctionArgument(c *FunctionArgumentContext)

	// EnterBuiltinFunctionStructure is called when entering the builtinFunctionStructure production.
	EnterBuiltinFunctionStructure(c *BuiltinFunctionStructureContext)

	// EnterFunctionName is called when entering the functionName production.
	EnterFunctionName(c *FunctionNameContext)

	// EnterCastExpression is called when entering the castExpression production.
	EnterCastExpression(c *CastExpressionContext)

	// EnterCaseExpression is called when entering the caseExpression production.
	EnterCaseExpression(c *CaseExpressionContext)

	// EnterWhenExpression is called when entering the whenExpression production.
	EnterWhenExpression(c *WhenExpressionContext)

	// EnterConstant is called when entering the constant production.
	EnterConstant(c *ConstantContext)

	// EnterSimpleStringLiteral is called when entering the simpleStringLiteral production.
	EnterSimpleStringLiteral(c *SimpleStringLiteralContext)

	// EnterStringLiteral is called when entering the stringLiteral production.
	EnterStringLiteral(c *StringLiteralContext)

	// EnterDoubleQuoteStringLiteral is called when entering the doubleQuoteStringLiteral production.
	EnterDoubleQuoteStringLiteral(c *DoubleQuoteStringLiteralContext)

	// EnterCharSetStringLiteral is called when entering the charSetStringLiteral production.
	EnterCharSetStringLiteral(c *CharSetStringLiteralContext)

	// EnterDateLiteral is called when entering the dateLiteral production.
	EnterDateLiteral(c *DateLiteralContext)

	// EnterDateTimeLiteral is called when entering the dateTimeLiteral production.
	EnterDateTimeLiteral(c *DateTimeLiteralContext)

	// EnterTimestampLiteral is called when entering the timestampLiteral production.
	EnterTimestampLiteral(c *TimestampLiteralContext)

	// EnterIntervalLiteral is called when entering the intervalLiteral production.
	EnterIntervalLiteral(c *IntervalLiteralContext)

	// EnterIntervalQualifiers is called when entering the intervalQualifiers production.
	EnterIntervalQualifiers(c *IntervalQualifiersContext)

	// EnterIntervalQualifiersUnit is called when entering the intervalQualifiersUnit production.
	EnterIntervalQualifiersUnit(c *IntervalQualifiersUnitContext)

	// EnterIntervalQualifierPrecision is called when entering the intervalQualifierPrecision production.
	EnterIntervalQualifierPrecision(c *IntervalQualifierPrecisionContext)

	// EnterBooleanValue is called when entering the booleanValue production.
	EnterBooleanValue(c *BooleanValueContext)

	// EnterTableOrPartition is called when entering the tableOrPartition production.
	EnterTableOrPartition(c *TableOrPartitionContext)

	// EnterPartitionSpec is called when entering the partitionSpec production.
	EnterPartitionSpec(c *PartitionSpecContext)

	// EnterPartitionVal is called when entering the partitionVal production.
	EnterPartitionVal(c *PartitionValContext)

	// EnterDateWithoutQuote is called when entering the dateWithoutQuote production.
	EnterDateWithoutQuote(c *DateWithoutQuoteContext)

	// EnterDropPartitionSpec is called when entering the dropPartitionSpec production.
	EnterDropPartitionSpec(c *DropPartitionSpecContext)

	// EnterSysFuncNames is called when entering the sysFuncNames production.
	EnterSysFuncNames(c *SysFuncNamesContext)

	// EnterDescFuncNames is called when entering the descFuncNames production.
	EnterDescFuncNames(c *DescFuncNamesContext)

	// EnterFunctionIdentifier is called when entering the functionIdentifier production.
	EnterFunctionIdentifier(c *FunctionIdentifierContext)

	// EnterReserved is called when entering the reserved production.
	EnterReserved(c *ReservedContext)

	// EnterNonReserved is called when entering the nonReserved production.
	EnterNonReserved(c *NonReservedContext)

	// EnterSql11ReservedKeywordsUsedAsCastFunctionName is called when entering the sql11ReservedKeywordsUsedAsCastFunctionName production.
	EnterSql11ReservedKeywordsUsedAsCastFunctionName(c *Sql11ReservedKeywordsUsedAsCastFunctionNameContext)

	// EnterSql11ReservedKeywordsUsedAsIdentifier is called when entering the sql11ReservedKeywordsUsedAsIdentifier production.
	EnterSql11ReservedKeywordsUsedAsIdentifier(c *Sql11ReservedKeywordsUsedAsIdentifierContext)

	// ExitScript is called when exiting the script production.
	ExitScript(c *ScriptContext)

	// ExitUserCodeBlock is called when exiting the userCodeBlock production.
	ExitUserCodeBlock(c *UserCodeBlockContext)

	// ExitStatement is called when exiting the statement production.
	ExitStatement(c *StatementContext)

	// ExitCompoundStatement is called when exiting the compoundStatement production.
	ExitCompoundStatement(c *CompoundStatementContext)

	// ExitNoopStatement is called when exiting the noopStatement production.
	ExitNoopStatement(c *NoopStatementContext)

	// ExitExecStatement is called when exiting the execStatement production.
	ExitExecStatement(c *ExecStatementContext)

	// ExitCteStatement is called when exiting the cteStatement production.
	ExitCteStatement(c *CteStatementContext)

	// ExitTableAliasWithCols is called when exiting the tableAliasWithCols production.
	ExitTableAliasWithCols(c *TableAliasWithColsContext)

	// ExitSubQuerySource is called when exiting the subQuerySource production.
	ExitSubQuerySource(c *SubQuerySourceContext)

	// ExitExplainStatement is called when exiting the explainStatement production.
	ExitExplainStatement(c *ExplainStatementContext)

	// ExitIfStatement is called when exiting the ifStatement production.
	ExitIfStatement(c *IfStatementContext)

	// ExitLoopStatement is called when exiting the loopStatement production.
	ExitLoopStatement(c *LoopStatementContext)

	// ExitFunctionDefinition is called when exiting the functionDefinition production.
	ExitFunctionDefinition(c *FunctionDefinitionContext)

	// ExitFunctionParameters is called when exiting the functionParameters production.
	ExitFunctionParameters(c *FunctionParametersContext)

	// ExitParameterDefinition is called when exiting the parameterDefinition production.
	ExitParameterDefinition(c *ParameterDefinitionContext)

	// ExitTypeDeclaration is called when exiting the typeDeclaration production.
	ExitTypeDeclaration(c *TypeDeclarationContext)

	// ExitParameterTypeDeclaration is called when exiting the parameterTypeDeclaration production.
	ExitParameterTypeDeclaration(c *ParameterTypeDeclarationContext)

	// ExitFunctionTypeDeclaration is called when exiting the functionTypeDeclaration production.
	ExitFunctionTypeDeclaration(c *FunctionTypeDeclarationContext)

	// ExitParameterTypeDeclarationList is called when exiting the parameterTypeDeclarationList production.
	ExitParameterTypeDeclarationList(c *ParameterTypeDeclarationListContext)

	// ExitParameterColumnNameTypeList is called when exiting the parameterColumnNameTypeList production.
	ExitParameterColumnNameTypeList(c *ParameterColumnNameTypeListContext)

	// ExitParameterColumnNameType is called when exiting the parameterColumnNameType production.
	ExitParameterColumnNameType(c *ParameterColumnNameTypeContext)

	// ExitVarSizeParam is called when exiting the varSizeParam production.
	ExitVarSizeParam(c *VarSizeParamContext)

	// ExitAssignStatement is called when exiting the assignStatement production.
	ExitAssignStatement(c *AssignStatementContext)

	// ExitPreSelectClauses is called when exiting the preSelectClauses production.
	ExitPreSelectClauses(c *PreSelectClausesContext)

	// ExitPostSelectClauses is called when exiting the postSelectClauses production.
	ExitPostSelectClauses(c *PostSelectClausesContext)

	// ExitSelectRest is called when exiting the selectRest production.
	ExitSelectRest(c *SelectRestContext)

	// ExitMultiInsertFromRest is called when exiting the multiInsertFromRest production.
	ExitMultiInsertFromRest(c *MultiInsertFromRestContext)

	// ExitFromRest is called when exiting the fromRest production.
	ExitFromRest(c *FromRestContext)

	// ExitSimpleQueryExpression is called when exiting the simpleQueryExpression production.
	ExitSimpleQueryExpression(c *SimpleQueryExpressionContext)

	// ExitSelectQueryExpression is called when exiting the selectQueryExpression production.
	ExitSelectQueryExpression(c *SelectQueryExpressionContext)

	// ExitFromQueryExpression is called when exiting the fromQueryExpression production.
	ExitFromQueryExpression(c *FromQueryExpressionContext)

	// ExitSetOperationFactor is called when exiting the setOperationFactor production.
	ExitSetOperationFactor(c *SetOperationFactorContext)

	// ExitQueryExpression is called when exiting the queryExpression production.
	ExitQueryExpression(c *QueryExpressionContext)

	// ExitQueryExpressionWithCTE is called when exiting the queryExpressionWithCTE production.
	ExitQueryExpressionWithCTE(c *QueryExpressionWithCTEContext)

	// ExitSetRHS is called when exiting the setRHS production.
	ExitSetRHS(c *SetRHSContext)

	// ExitMultiInsertSetOperationFactor is called when exiting the multiInsertSetOperationFactor production.
	ExitMultiInsertSetOperationFactor(c *MultiInsertSetOperationFactorContext)

	// ExitMultiInsertSelect is called when exiting the multiInsertSelect production.
	ExitMultiInsertSelect(c *MultiInsertSelectContext)

	// ExitMultiInsertSetRHS is called when exiting the multiInsertSetRHS production.
	ExitMultiInsertSetRHS(c *MultiInsertSetRHSContext)

	// ExitMultiInsertBranch is called when exiting the multiInsertBranch production.
	ExitMultiInsertBranch(c *MultiInsertBranchContext)

	// ExitFromStatement is called when exiting the fromStatement production.
	ExitFromStatement(c *FromStatementContext)

	// ExitInsertStatement is called when exiting the insertStatement production.
	ExitInsertStatement(c *InsertStatementContext)

	// ExitSelectQueryStatement is called when exiting the selectQueryStatement production.
	ExitSelectQueryStatement(c *SelectQueryStatementContext)

	// ExitQueryStatement is called when exiting the queryStatement production.
	ExitQueryStatement(c *QueryStatementContext)

	// ExitInsertStatementWithCTE is called when exiting the insertStatementWithCTE production.
	ExitInsertStatementWithCTE(c *InsertStatementWithCTEContext)

	// ExitSubQueryExpression is called when exiting the subQueryExpression production.
	ExitSubQueryExpression(c *SubQueryExpressionContext)

	// ExitLimitClause is called when exiting the limitClause production.
	ExitLimitClause(c *LimitClauseContext)

	// ExitFromSource is called when exiting the fromSource production.
	ExitFromSource(c *FromSourceContext)

	// ExitTableVariableSource is called when exiting the tableVariableSource production.
	ExitTableVariableSource(c *TableVariableSourceContext)

	// ExitTableFunctionSource is called when exiting the tableFunctionSource production.
	ExitTableFunctionSource(c *TableFunctionSourceContext)

	// ExitCreateMachineLearningModelStatment is called when exiting the createMachineLearningModelStatment production.
	ExitCreateMachineLearningModelStatment(c *CreateMachineLearningModelStatmentContext)

	// ExitVariableName is called when exiting the variableName production.
	ExitVariableName(c *VariableNameContext)

	// ExitAtomExpression is called when exiting the atomExpression production.
	ExitAtomExpression(c *AtomExpressionContext)

	// ExitVariableRef is called when exiting the variableRef production.
	ExitVariableRef(c *VariableRefContext)

	// ExitVariableCall is called when exiting the variableCall production.
	ExitVariableCall(c *VariableCallContext)

	// ExitFunNameRef is called when exiting the funNameRef production.
	ExitFunNameRef(c *FunNameRefContext)

	// ExitLambdaExpression is called when exiting the lambdaExpression production.
	ExitLambdaExpression(c *LambdaExpressionContext)

	// ExitLambdaParameter is called when exiting the lambdaParameter production.
	ExitLambdaParameter(c *LambdaParameterContext)

	// ExitTableOrColumnRef is called when exiting the tableOrColumnRef production.
	ExitTableOrColumnRef(c *TableOrColumnRefContext)

	// ExitConstructExpression is called when exiting the constructExpression production.
	ExitConstructExpression(c *ConstructExpressionContext)

	// ExitExistsExpression is called when exiting the existsExpression production.
	ExitExistsExpression(c *ExistsExpressionContext)

	// ExitScalarSubQueryExpression is called when exiting the scalarSubQueryExpression production.
	ExitScalarSubQueryExpression(c *ScalarSubQueryExpressionContext)

	// ExitClassNameWithPackage is called when exiting the classNameWithPackage production.
	ExitClassNameWithPackage(c *ClassNameWithPackageContext)

	// ExitClassNameOrArrayDecl is called when exiting the classNameOrArrayDecl production.
	ExitClassNameOrArrayDecl(c *ClassNameOrArrayDeclContext)

	// ExitClassNameList is called when exiting the classNameList production.
	ExitClassNameList(c *ClassNameListContext)

	// ExitOdpsqlNonReserved is called when exiting the odpsqlNonReserved production.
	ExitOdpsqlNonReserved(c *OdpsqlNonReservedContext)

	// ExitRelaxedKeywords is called when exiting the relaxedKeywords production.
	ExitRelaxedKeywords(c *RelaxedKeywordsContext)

	// ExitAllIdentifiers is called when exiting the allIdentifiers production.
	ExitAllIdentifiers(c *AllIdentifiersContext)

	// ExitIdentifier is called when exiting the identifier production.
	ExitIdentifier(c *IdentifierContext)

	// ExitAliasIdentifier is called when exiting the aliasIdentifier production.
	ExitAliasIdentifier(c *AliasIdentifierContext)

	// ExitIdentifierWithoutSql11 is called when exiting the identifierWithoutSql11 production.
	ExitIdentifierWithoutSql11(c *IdentifierWithoutSql11Context)

	// ExitAlterTableChangeOwner is called when exiting the alterTableChangeOwner production.
	ExitAlterTableChangeOwner(c *AlterTableChangeOwnerContext)

	// ExitAlterViewChangeOwner is called when exiting the alterViewChangeOwner production.
	ExitAlterViewChangeOwner(c *AlterViewChangeOwnerContext)

	// ExitAlterTableEnableHubTable is called when exiting the alterTableEnableHubTable production.
	ExitAlterTableEnableHubTable(c *AlterTableEnableHubTableContext)

	// ExitTableLifecycle is called when exiting the tableLifecycle production.
	ExitTableLifecycle(c *TableLifecycleContext)

	// ExitSetStatement is called when exiting the setStatement production.
	ExitSetStatement(c *SetStatementContext)

	// ExitAnythingButEqualOrSemi is called when exiting the anythingButEqualOrSemi production.
	ExitAnythingButEqualOrSemi(c *AnythingButEqualOrSemiContext)

	// ExitAnythingButSemi is called when exiting the anythingButSemi production.
	ExitAnythingButSemi(c *AnythingButSemiContext)

	// ExitSetProjectStatement is called when exiting the setProjectStatement production.
	ExitSetProjectStatement(c *SetProjectStatementContext)

	// ExitLabel is called when exiting the label production.
	ExitLabel(c *LabelContext)

	// ExitSkewInfoVal is called when exiting the skewInfoVal production.
	ExitSkewInfoVal(c *SkewInfoValContext)

	// ExitMemberAccessOperator is called when exiting the memberAccessOperator production.
	ExitMemberAccessOperator(c *MemberAccessOperatorContext)

	// ExitMethodAccessOperator is called when exiting the methodAccessOperator production.
	ExitMethodAccessOperator(c *MethodAccessOperatorContext)

	// ExitIsNullOperator is called when exiting the isNullOperator production.
	ExitIsNullOperator(c *IsNullOperatorContext)

	// ExitInOperator is called when exiting the inOperator production.
	ExitInOperator(c *InOperatorContext)

	// ExitBetweenOperator is called when exiting the betweenOperator production.
	ExitBetweenOperator(c *BetweenOperatorContext)

	// ExitMathExpression is called when exiting the mathExpression production.
	ExitMathExpression(c *MathExpressionContext)

	// ExitUnarySuffixExpression is called when exiting the unarySuffixExpression production.
	ExitUnarySuffixExpression(c *UnarySuffixExpressionContext)

	// ExitUnaryPrefixExpression is called when exiting the unaryPrefixExpression production.
	ExitUnaryPrefixExpression(c *UnaryPrefixExpressionContext)

	// ExitFieldExpression is called when exiting the fieldExpression production.
	ExitFieldExpression(c *FieldExpressionContext)

	// ExitLogicalExpression is called when exiting the logicalExpression production.
	ExitLogicalExpression(c *LogicalExpressionContext)

	// ExitNotExpression is called when exiting the notExpression production.
	ExitNotExpression(c *NotExpressionContext)

	// ExitEqualExpression is called when exiting the equalExpression production.
	ExitEqualExpression(c *EqualExpressionContext)

	// ExitMathExpressionListInParentheses is called when exiting the mathExpressionListInParentheses production.
	ExitMathExpressionListInParentheses(c *MathExpressionListInParenthesesContext)

	// ExitMathExpressionList is called when exiting the mathExpressionList production.
	ExitMathExpressionList(c *MathExpressionListContext)

	// ExitExpression is called when exiting the expression production.
	ExitExpression(c *ExpressionContext)

	// ExitStatisticStatement is called when exiting the statisticStatement production.
	ExitStatisticStatement(c *StatisticStatementContext)

	// ExitAddRemoveStatisticStatement is called when exiting the addRemoveStatisticStatement production.
	ExitAddRemoveStatisticStatement(c *AddRemoveStatisticStatementContext)

	// ExitStatisticInfo is called when exiting the statisticInfo production.
	ExitStatisticInfo(c *StatisticInfoContext)

	// ExitShowStatisticStatement is called when exiting the showStatisticStatement production.
	ExitShowStatisticStatement(c *ShowStatisticStatementContext)

	// ExitShowStatisticListStatement is called when exiting the showStatisticListStatement production.
	ExitShowStatisticListStatement(c *ShowStatisticListStatementContext)

	// ExitCountTableStatement is called when exiting the countTableStatement production.
	ExitCountTableStatement(c *CountTableStatementContext)

	// ExitStatisticName is called when exiting the statisticName production.
	ExitStatisticName(c *StatisticNameContext)

	// ExitInstanceManagement is called when exiting the instanceManagement production.
	ExitInstanceManagement(c *InstanceManagementContext)

	// ExitInstanceStatus is called when exiting the instanceStatus production.
	ExitInstanceStatus(c *InstanceStatusContext)

	// ExitKillInstance is called when exiting the killInstance production.
	ExitKillInstance(c *KillInstanceContext)

	// ExitInstanceId is called when exiting the instanceId production.
	ExitInstanceId(c *InstanceIdContext)

	// ExitResourceManagement is called when exiting the resourceManagement production.
	ExitResourceManagement(c *ResourceManagementContext)

	// ExitAddResource is called when exiting the addResource production.
	ExitAddResource(c *AddResourceContext)

	// ExitDropResource is called when exiting the dropResource production.
	ExitDropResource(c *DropResourceContext)

	// ExitResourceId is called when exiting the resourceId production.
	ExitResourceId(c *ResourceIdContext)

	// ExitDropOfflineModel is called when exiting the dropOfflineModel production.
	ExitDropOfflineModel(c *DropOfflineModelContext)

	// ExitGetResource is called when exiting the getResource production.
	ExitGetResource(c *GetResourceContext)

	// ExitOptions is called when exiting the options production.
	ExitOptions(c *OptionsContext)

	// ExitAuthorizationStatement is called when exiting the authorizationStatement production.
	ExitAuthorizationStatement(c *AuthorizationStatementContext)

	// ExitListUsers is called when exiting the listUsers production.
	ExitListUsers(c *ListUsersContext)

	// ExitListGroups is called when exiting the listGroups production.
	ExitListGroups(c *ListGroupsContext)

	// ExitAddUserStatement is called when exiting the addUserStatement production.
	ExitAddUserStatement(c *AddUserStatementContext)

	// ExitAddGroupStatement is called when exiting the addGroupStatement production.
	ExitAddGroupStatement(c *AddGroupStatementContext)

	// ExitRemoveUserStatement is called when exiting the removeUserStatement production.
	ExitRemoveUserStatement(c *RemoveUserStatementContext)

	// ExitRemoveGroupStatement is called when exiting the removeGroupStatement production.
	ExitRemoveGroupStatement(c *RemoveGroupStatementContext)

	// ExitAddAccountProvider is called when exiting the addAccountProvider production.
	ExitAddAccountProvider(c *AddAccountProviderContext)

	// ExitRemoveAccountProvider is called when exiting the removeAccountProvider production.
	ExitRemoveAccountProvider(c *RemoveAccountProviderContext)

	// ExitShowAcl is called when exiting the showAcl production.
	ExitShowAcl(c *ShowAclContext)

	// ExitListRoles is called when exiting the listRoles production.
	ExitListRoles(c *ListRolesContext)

	// ExitWhoami is called when exiting the whoami production.
	ExitWhoami(c *WhoamiContext)

	// ExitListTrustedProjects is called when exiting the listTrustedProjects production.
	ExitListTrustedProjects(c *ListTrustedProjectsContext)

	// ExitAddTrustedProject is called when exiting the addTrustedProject production.
	ExitAddTrustedProject(c *AddTrustedProjectContext)

	// ExitRemoveTrustedProject is called when exiting the removeTrustedProject production.
	ExitRemoveTrustedProject(c *RemoveTrustedProjectContext)

	// ExitShowSecurityConfiguration is called when exiting the showSecurityConfiguration production.
	ExitShowSecurityConfiguration(c *ShowSecurityConfigurationContext)

	// ExitShowPackages is called when exiting the showPackages production.
	ExitShowPackages(c *ShowPackagesContext)

	// ExitShowItems is called when exiting the showItems production.
	ExitShowItems(c *ShowItemsContext)

	// ExitInstallPackage is called when exiting the installPackage production.
	ExitInstallPackage(c *InstallPackageContext)

	// ExitUninstallPackage is called when exiting the uninstallPackage production.
	ExitUninstallPackage(c *UninstallPackageContext)

	// ExitCreatePackage is called when exiting the createPackage production.
	ExitCreatePackage(c *CreatePackageContext)

	// ExitDeletePackage is called when exiting the deletePackage production.
	ExitDeletePackage(c *DeletePackageContext)

	// ExitAddToPackage is called when exiting the addToPackage production.
	ExitAddToPackage(c *AddToPackageContext)

	// ExitRemoveFromPackage is called when exiting the removeFromPackage production.
	ExitRemoveFromPackage(c *RemoveFromPackageContext)

	// ExitAllowPackage is called when exiting the allowPackage production.
	ExitAllowPackage(c *AllowPackageContext)

	// ExitDisallowPackage is called when exiting the disallowPackage production.
	ExitDisallowPackage(c *DisallowPackageContext)

	// ExitPutPolicy is called when exiting the putPolicy production.
	ExitPutPolicy(c *PutPolicyContext)

	// ExitGetPolicy is called when exiting the getPolicy production.
	ExitGetPolicy(c *GetPolicyContext)

	// ExitClearExpiredGrants is called when exiting the clearExpiredGrants production.
	ExitClearExpiredGrants(c *ClearExpiredGrantsContext)

	// ExitGrantLabel is called when exiting the grantLabel production.
	ExitGrantLabel(c *GrantLabelContext)

	// ExitRevokeLabel is called when exiting the revokeLabel production.
	ExitRevokeLabel(c *RevokeLabelContext)

	// ExitShowLabel is called when exiting the showLabel production.
	ExitShowLabel(c *ShowLabelContext)

	// ExitGrantSuperPrivilege is called when exiting the grantSuperPrivilege production.
	ExitGrantSuperPrivilege(c *GrantSuperPrivilegeContext)

	// ExitRevokeSuperPrivilege is called when exiting the revokeSuperPrivilege production.
	ExitRevokeSuperPrivilege(c *RevokeSuperPrivilegeContext)

	// ExitCreateRoleStatement is called when exiting the createRoleStatement production.
	ExitCreateRoleStatement(c *CreateRoleStatementContext)

	// ExitDropRoleStatement is called when exiting the dropRoleStatement production.
	ExitDropRoleStatement(c *DropRoleStatementContext)

	// ExitAddRoleToProject is called when exiting the addRoleToProject production.
	ExitAddRoleToProject(c *AddRoleToProjectContext)

	// ExitRemoveRoleFromProject is called when exiting the removeRoleFromProject production.
	ExitRemoveRoleFromProject(c *RemoveRoleFromProjectContext)

	// ExitGrantRole is called when exiting the grantRole production.
	ExitGrantRole(c *GrantRoleContext)

	// ExitRevokeRole is called when exiting the revokeRole production.
	ExitRevokeRole(c *RevokeRoleContext)

	// ExitGrantPrivileges is called when exiting the grantPrivileges production.
	ExitGrantPrivileges(c *GrantPrivilegesContext)

	// ExitPrivilegeProperties is called when exiting the privilegeProperties production.
	ExitPrivilegeProperties(c *PrivilegePropertiesContext)

	// ExitPrivilegePropertieKeys is called when exiting the privilegePropertieKeys production.
	ExitPrivilegePropertieKeys(c *PrivilegePropertieKeysContext)

	// ExitRevokePrivileges is called when exiting the revokePrivileges production.
	ExitRevokePrivileges(c *RevokePrivilegesContext)

	// ExitPurgePrivileges is called when exiting the purgePrivileges production.
	ExitPurgePrivileges(c *PurgePrivilegesContext)

	// ExitShowGrants is called when exiting the showGrants production.
	ExitShowGrants(c *ShowGrantsContext)

	// ExitShowRoleGrants is called when exiting the showRoleGrants production.
	ExitShowRoleGrants(c *ShowRoleGrantsContext)

	// ExitShowRoles is called when exiting the showRoles production.
	ExitShowRoles(c *ShowRolesContext)

	// ExitShowRolePrincipals is called when exiting the showRolePrincipals production.
	ExitShowRolePrincipals(c *ShowRolePrincipalsContext)

	// ExitUser is called when exiting the user production.
	ExitUser(c *UserContext)

	// ExitUserRoleComments is called when exiting the userRoleComments production.
	ExitUserRoleComments(c *UserRoleCommentsContext)

	// ExitAccountProvider is called when exiting the accountProvider production.
	ExitAccountProvider(c *AccountProviderContext)

	// ExitProjectName is called when exiting the projectName production.
	ExitProjectName(c *ProjectNameContext)

	// ExitPrivilegeObjectName is called when exiting the privilegeObjectName production.
	ExitPrivilegeObjectName(c *PrivilegeObjectNameContext)

	// ExitPrivilegeObjectType is called when exiting the privilegeObjectType production.
	ExitPrivilegeObjectType(c *PrivilegeObjectTypeContext)

	// ExitRoleName is called when exiting the roleName production.
	ExitRoleName(c *RoleNameContext)

	// ExitPackageName is called when exiting the packageName production.
	ExitPackageName(c *PackageNameContext)

	// ExitPackageNameWithProject is called when exiting the packageNameWithProject production.
	ExitPackageNameWithProject(c *PackageNameWithProjectContext)

	// ExitPrincipalSpecification is called when exiting the principalSpecification production.
	ExitPrincipalSpecification(c *PrincipalSpecificationContext)

	// ExitPrincipalName is called when exiting the principalName production.
	ExitPrincipalName(c *PrincipalNameContext)

	// ExitPrincipalIdentifier is called when exiting the principalIdentifier production.
	ExitPrincipalIdentifier(c *PrincipalIdentifierContext)

	// ExitPrivilege is called when exiting the privilege production.
	ExitPrivilege(c *PrivilegeContext)

	// ExitPrivilegeType is called when exiting the privilegeType production.
	ExitPrivilegeType(c *PrivilegeTypeContext)

	// ExitPrivilegeObject is called when exiting the privilegeObject production.
	ExitPrivilegeObject(c *PrivilegeObjectContext)

	// ExitFilePath is called when exiting the filePath production.
	ExitFilePath(c *FilePathContext)

	// ExitPolicyCondition is called when exiting the policyCondition production.
	ExitPolicyCondition(c *PolicyConditionContext)

	// ExitPolicyConditionOp is called when exiting the policyConditionOp production.
	ExitPolicyConditionOp(c *PolicyConditionOpContext)

	// ExitPolicyKey is called when exiting the policyKey production.
	ExitPolicyKey(c *PolicyKeyContext)

	// ExitPolicyValue is called when exiting the policyValue production.
	ExitPolicyValue(c *PolicyValueContext)

	// ExitShowCurrentRole is called when exiting the showCurrentRole production.
	ExitShowCurrentRole(c *ShowCurrentRoleContext)

	// ExitSetRole is called when exiting the setRole production.
	ExitSetRole(c *SetRoleContext)

	// ExitAdminOptionFor is called when exiting the adminOptionFor production.
	ExitAdminOptionFor(c *AdminOptionForContext)

	// ExitWithAdminOption is called when exiting the withAdminOption production.
	ExitWithAdminOption(c *WithAdminOptionContext)

	// ExitWithGrantOption is called when exiting the withGrantOption production.
	ExitWithGrantOption(c *WithGrantOptionContext)

	// ExitGrantOptionFor is called when exiting the grantOptionFor production.
	ExitGrantOptionFor(c *GrantOptionForContext)

	// ExitExplainOption is called when exiting the explainOption production.
	ExitExplainOption(c *ExplainOptionContext)

	// ExitLoadStatement is called when exiting the loadStatement production.
	ExitLoadStatement(c *LoadStatementContext)

	// ExitReplicationClause is called when exiting the replicationClause production.
	ExitReplicationClause(c *ReplicationClauseContext)

	// ExitExportStatement is called when exiting the exportStatement production.
	ExitExportStatement(c *ExportStatementContext)

	// ExitImportStatement is called when exiting the importStatement production.
	ExitImportStatement(c *ImportStatementContext)

	// ExitReadStatement is called when exiting the readStatement production.
	ExitReadStatement(c *ReadStatementContext)

	// ExitUndoStatement is called when exiting the undoStatement production.
	ExitUndoStatement(c *UndoStatementContext)

	// ExitRedoStatement is called when exiting the redoStatement production.
	ExitRedoStatement(c *RedoStatementContext)

	// ExitPurgeStatement is called when exiting the purgeStatement production.
	ExitPurgeStatement(c *PurgeStatementContext)

	// ExitDropTableVairableStatement is called when exiting the dropTableVairableStatement production.
	ExitDropTableVairableStatement(c *DropTableVairableStatementContext)

	// ExitMsckRepairTableStatement is called when exiting the msckRepairTableStatement production.
	ExitMsckRepairTableStatement(c *MsckRepairTableStatementContext)

	// ExitDdlStatement is called when exiting the ddlStatement production.
	ExitDdlStatement(c *DdlStatementContext)

	// ExitPartitionSpecOrPartitionId is called when exiting the partitionSpecOrPartitionId production.
	ExitPartitionSpecOrPartitionId(c *PartitionSpecOrPartitionIdContext)

	// ExitTableOrTableId is called when exiting the tableOrTableId production.
	ExitTableOrTableId(c *TableOrTableIdContext)

	// ExitTableHistoryStatement is called when exiting the tableHistoryStatement production.
	ExitTableHistoryStatement(c *TableHistoryStatementContext)

	// ExitSetExstore is called when exiting the setExstore production.
	ExitSetExstore(c *SetExstoreContext)

	// ExitIfExists is called when exiting the ifExists production.
	ExitIfExists(c *IfExistsContext)

	// ExitRestrictOrCascade is called when exiting the restrictOrCascade production.
	ExitRestrictOrCascade(c *RestrictOrCascadeContext)

	// ExitIfNotExists is called when exiting the ifNotExists production.
	ExitIfNotExists(c *IfNotExistsContext)

	// ExitRewriteEnabled is called when exiting the rewriteEnabled production.
	ExitRewriteEnabled(c *RewriteEnabledContext)

	// ExitRewriteDisabled is called when exiting the rewriteDisabled production.
	ExitRewriteDisabled(c *RewriteDisabledContext)

	// ExitStoredAsDirs is called when exiting the storedAsDirs production.
	ExitStoredAsDirs(c *StoredAsDirsContext)

	// ExitOrReplace is called when exiting the orReplace production.
	ExitOrReplace(c *OrReplaceContext)

	// ExitIgnoreProtection is called when exiting the ignoreProtection production.
	ExitIgnoreProtection(c *IgnoreProtectionContext)

	// ExitCreateDatabaseStatement is called when exiting the createDatabaseStatement production.
	ExitCreateDatabaseStatement(c *CreateDatabaseStatementContext)

	// ExitSchemaName is called when exiting the schemaName production.
	ExitSchemaName(c *SchemaNameContext)

	// ExitCreateSchemaStatement is called when exiting the createSchemaStatement production.
	ExitCreateSchemaStatement(c *CreateSchemaStatementContext)

	// ExitDbLocation is called when exiting the dbLocation production.
	ExitDbLocation(c *DbLocationContext)

	// ExitDbProperties is called when exiting the dbProperties production.
	ExitDbProperties(c *DbPropertiesContext)

	// ExitDbPropertiesList is called when exiting the dbPropertiesList production.
	ExitDbPropertiesList(c *DbPropertiesListContext)

	// ExitSwitchDatabaseStatement is called when exiting the switchDatabaseStatement production.
	ExitSwitchDatabaseStatement(c *SwitchDatabaseStatementContext)

	// ExitDropDatabaseStatement is called when exiting the dropDatabaseStatement production.
	ExitDropDatabaseStatement(c *DropDatabaseStatementContext)

	// ExitDropSchemaStatement is called when exiting the dropSchemaStatement production.
	ExitDropSchemaStatement(c *DropSchemaStatementContext)

	// ExitDatabaseComment is called when exiting the databaseComment production.
	ExitDatabaseComment(c *DatabaseCommentContext)

	// ExitDataFormatDesc is called when exiting the dataFormatDesc production.
	ExitDataFormatDesc(c *DataFormatDescContext)

	// ExitCreateTableStatement is called when exiting the createTableStatement production.
	ExitCreateTableStatement(c *CreateTableStatementContext)

	// ExitTruncateTableStatement is called when exiting the truncateTableStatement production.
	ExitTruncateTableStatement(c *TruncateTableStatementContext)

	// ExitCreateIndexStatement is called when exiting the createIndexStatement production.
	ExitCreateIndexStatement(c *CreateIndexStatementContext)

	// ExitIndexComment is called when exiting the indexComment production.
	ExitIndexComment(c *IndexCommentContext)

	// ExitAutoRebuild is called when exiting the autoRebuild production.
	ExitAutoRebuild(c *AutoRebuildContext)

	// ExitIndexTblName is called when exiting the indexTblName production.
	ExitIndexTblName(c *IndexTblNameContext)

	// ExitIndexPropertiesPrefixed is called when exiting the indexPropertiesPrefixed production.
	ExitIndexPropertiesPrefixed(c *IndexPropertiesPrefixedContext)

	// ExitIndexProperties is called when exiting the indexProperties production.
	ExitIndexProperties(c *IndexPropertiesContext)

	// ExitIndexPropertiesList is called when exiting the indexPropertiesList production.
	ExitIndexPropertiesList(c *IndexPropertiesListContext)

	// ExitDropIndexStatement is called when exiting the dropIndexStatement production.
	ExitDropIndexStatement(c *DropIndexStatementContext)

	// ExitDropTableStatement is called when exiting the dropTableStatement production.
	ExitDropTableStatement(c *DropTableStatementContext)

	// ExitAlterStatement is called when exiting the alterStatement production.
	ExitAlterStatement(c *AlterStatementContext)

	// ExitAlterSchemaStatementSuffix is called when exiting the alterSchemaStatementSuffix production.
	ExitAlterSchemaStatementSuffix(c *AlterSchemaStatementSuffixContext)

	// ExitAlterTableStatementSuffix is called when exiting the alterTableStatementSuffix production.
	ExitAlterTableStatementSuffix(c *AlterTableStatementSuffixContext)

	// ExitAlterTableMergePartitionSuffix is called when exiting the alterTableMergePartitionSuffix production.
	ExitAlterTableMergePartitionSuffix(c *AlterTableMergePartitionSuffixContext)

	// ExitAlterStatementSuffixAddConstraint is called when exiting the alterStatementSuffixAddConstraint production.
	ExitAlterStatementSuffixAddConstraint(c *AlterStatementSuffixAddConstraintContext)

	// ExitAlterTblPartitionStatementSuffix is called when exiting the alterTblPartitionStatementSuffix production.
	ExitAlterTblPartitionStatementSuffix(c *AlterTblPartitionStatementSuffixContext)

	// ExitAlterStatementSuffixPartitionLifecycle is called when exiting the alterStatementSuffixPartitionLifecycle production.
	ExitAlterStatementSuffixPartitionLifecycle(c *AlterStatementSuffixPartitionLifecycleContext)

	// ExitAlterTblPartitionStatementSuffixProperties is called when exiting the alterTblPartitionStatementSuffixProperties production.
	ExitAlterTblPartitionStatementSuffixProperties(c *AlterTblPartitionStatementSuffixPropertiesContext)

	// ExitAlterStatementPartitionKeyType is called when exiting the alterStatementPartitionKeyType production.
	ExitAlterStatementPartitionKeyType(c *AlterStatementPartitionKeyTypeContext)

	// ExitAlterViewStatementSuffix is called when exiting the alterViewStatementSuffix production.
	ExitAlterViewStatementSuffix(c *AlterViewStatementSuffixContext)

	// ExitAlterMaterializedViewStatementSuffix is called when exiting the alterMaterializedViewStatementSuffix production.
	ExitAlterMaterializedViewStatementSuffix(c *AlterMaterializedViewStatementSuffixContext)

	// ExitAlterMaterializedViewSuffixRewrite is called when exiting the alterMaterializedViewSuffixRewrite production.
	ExitAlterMaterializedViewSuffixRewrite(c *AlterMaterializedViewSuffixRewriteContext)

	// ExitAlterMaterializedViewSuffixRebuild is called when exiting the alterMaterializedViewSuffixRebuild production.
	ExitAlterMaterializedViewSuffixRebuild(c *AlterMaterializedViewSuffixRebuildContext)

	// ExitAlterIndexStatementSuffix is called when exiting the alterIndexStatementSuffix production.
	ExitAlterIndexStatementSuffix(c *AlterIndexStatementSuffixContext)

	// ExitAlterDatabaseStatementSuffix is called when exiting the alterDatabaseStatementSuffix production.
	ExitAlterDatabaseStatementSuffix(c *AlterDatabaseStatementSuffixContext)

	// ExitAlterDatabaseSuffixProperties is called when exiting the alterDatabaseSuffixProperties production.
	ExitAlterDatabaseSuffixProperties(c *AlterDatabaseSuffixPropertiesContext)

	// ExitAlterDatabaseSuffixSetOwner is called when exiting the alterDatabaseSuffixSetOwner production.
	ExitAlterDatabaseSuffixSetOwner(c *AlterDatabaseSuffixSetOwnerContext)

	// ExitAlterStatementSuffixRename is called when exiting the alterStatementSuffixRename production.
	ExitAlterStatementSuffixRename(c *AlterStatementSuffixRenameContext)

	// ExitAlterStatementSuffixAddCol is called when exiting the alterStatementSuffixAddCol production.
	ExitAlterStatementSuffixAddCol(c *AlterStatementSuffixAddColContext)

	// ExitAlterStatementSuffixRenameCol is called when exiting the alterStatementSuffixRenameCol production.
	ExitAlterStatementSuffixRenameCol(c *AlterStatementSuffixRenameColContext)

	// ExitAlterStatementSuffixDropCol is called when exiting the alterStatementSuffixDropCol production.
	ExitAlterStatementSuffixDropCol(c *AlterStatementSuffixDropColContext)

	// ExitAlterStatementSuffixUpdateStatsCol is called when exiting the alterStatementSuffixUpdateStatsCol production.
	ExitAlterStatementSuffixUpdateStatsCol(c *AlterStatementSuffixUpdateStatsColContext)

	// ExitAlterStatementChangeColPosition is called when exiting the alterStatementChangeColPosition production.
	ExitAlterStatementChangeColPosition(c *AlterStatementChangeColPositionContext)

	// ExitAlterStatementSuffixAddPartitions is called when exiting the alterStatementSuffixAddPartitions production.
	ExitAlterStatementSuffixAddPartitions(c *AlterStatementSuffixAddPartitionsContext)

	// ExitAlterStatementSuffixAddPartitionsElement is called when exiting the alterStatementSuffixAddPartitionsElement production.
	ExitAlterStatementSuffixAddPartitionsElement(c *AlterStatementSuffixAddPartitionsElementContext)

	// ExitAlterStatementSuffixTouch is called when exiting the alterStatementSuffixTouch production.
	ExitAlterStatementSuffixTouch(c *AlterStatementSuffixTouchContext)

	// ExitAlterStatementSuffixArchive is called when exiting the alterStatementSuffixArchive production.
	ExitAlterStatementSuffixArchive(c *AlterStatementSuffixArchiveContext)

	// ExitAlterStatementSuffixUnArchive is called when exiting the alterStatementSuffixUnArchive production.
	ExitAlterStatementSuffixUnArchive(c *AlterStatementSuffixUnArchiveContext)

	// ExitAlterStatementSuffixChangeOwner is called when exiting the alterStatementSuffixChangeOwner production.
	ExitAlterStatementSuffixChangeOwner(c *AlterStatementSuffixChangeOwnerContext)

	// ExitPartitionLocation is called when exiting the partitionLocation production.
	ExitPartitionLocation(c *PartitionLocationContext)

	// ExitAlterStatementSuffixDropPartitions is called when exiting the alterStatementSuffixDropPartitions production.
	ExitAlterStatementSuffixDropPartitions(c *AlterStatementSuffixDropPartitionsContext)

	// ExitAlterStatementSuffixProperties is called when exiting the alterStatementSuffixProperties production.
	ExitAlterStatementSuffixProperties(c *AlterStatementSuffixPropertiesContext)

	// ExitAlterViewSuffixProperties is called when exiting the alterViewSuffixProperties production.
	ExitAlterViewSuffixProperties(c *AlterViewSuffixPropertiesContext)

	// ExitAlterViewColumnCommentSuffix is called when exiting the alterViewColumnCommentSuffix production.
	ExitAlterViewColumnCommentSuffix(c *AlterViewColumnCommentSuffixContext)

	// ExitAlterStatementSuffixSerdeProperties is called when exiting the alterStatementSuffixSerdeProperties production.
	ExitAlterStatementSuffixSerdeProperties(c *AlterStatementSuffixSerdePropertiesContext)

	// ExitTablePartitionPrefix is called when exiting the tablePartitionPrefix production.
	ExitTablePartitionPrefix(c *TablePartitionPrefixContext)

	// ExitAlterStatementSuffixFileFormat is called when exiting the alterStatementSuffixFileFormat production.
	ExitAlterStatementSuffixFileFormat(c *AlterStatementSuffixFileFormatContext)

	// ExitAlterStatementSuffixClusterbySortby is called when exiting the alterStatementSuffixClusterbySortby production.
	ExitAlterStatementSuffixClusterbySortby(c *AlterStatementSuffixClusterbySortbyContext)

	// ExitAlterTblPartitionStatementSuffixSkewedLocation is called when exiting the alterTblPartitionStatementSuffixSkewedLocation production.
	ExitAlterTblPartitionStatementSuffixSkewedLocation(c *AlterTblPartitionStatementSuffixSkewedLocationContext)

	// ExitSkewedLocations is called when exiting the skewedLocations production.
	ExitSkewedLocations(c *SkewedLocationsContext)

	// ExitSkewedLocationsList is called when exiting the skewedLocationsList production.
	ExitSkewedLocationsList(c *SkewedLocationsListContext)

	// ExitSkewedLocationMap is called when exiting the skewedLocationMap production.
	ExitSkewedLocationMap(c *SkewedLocationMapContext)

	// ExitAlterStatementSuffixLocation is called when exiting the alterStatementSuffixLocation production.
	ExitAlterStatementSuffixLocation(c *AlterStatementSuffixLocationContext)

	// ExitAlterStatementSuffixSkewedby is called when exiting the alterStatementSuffixSkewedby production.
	ExitAlterStatementSuffixSkewedby(c *AlterStatementSuffixSkewedbyContext)

	// ExitAlterStatementSuffixExchangePartition is called when exiting the alterStatementSuffixExchangePartition production.
	ExitAlterStatementSuffixExchangePartition(c *AlterStatementSuffixExchangePartitionContext)

	// ExitAlterStatementSuffixProtectMode is called when exiting the alterStatementSuffixProtectMode production.
	ExitAlterStatementSuffixProtectMode(c *AlterStatementSuffixProtectModeContext)

	// ExitAlterStatementSuffixRenamePart is called when exiting the alterStatementSuffixRenamePart production.
	ExitAlterStatementSuffixRenamePart(c *AlterStatementSuffixRenamePartContext)

	// ExitAlterStatementSuffixStatsPart is called when exiting the alterStatementSuffixStatsPart production.
	ExitAlterStatementSuffixStatsPart(c *AlterStatementSuffixStatsPartContext)

	// ExitAlterStatementSuffixMergeFiles is called when exiting the alterStatementSuffixMergeFiles production.
	ExitAlterStatementSuffixMergeFiles(c *AlterStatementSuffixMergeFilesContext)

	// ExitAlterProtectMode is called when exiting the alterProtectMode production.
	ExitAlterProtectMode(c *AlterProtectModeContext)

	// ExitAlterProtectModeMode is called when exiting the alterProtectModeMode production.
	ExitAlterProtectModeMode(c *AlterProtectModeModeContext)

	// ExitAlterStatementSuffixBucketNum is called when exiting the alterStatementSuffixBucketNum production.
	ExitAlterStatementSuffixBucketNum(c *AlterStatementSuffixBucketNumContext)

	// ExitAlterStatementSuffixCompact is called when exiting the alterStatementSuffixCompact production.
	ExitAlterStatementSuffixCompact(c *AlterStatementSuffixCompactContext)

	// ExitFileFormat is called when exiting the fileFormat production.
	ExitFileFormat(c *FileFormatContext)

	// ExitTabTypeExpr is called when exiting the tabTypeExpr production.
	ExitTabTypeExpr(c *TabTypeExprContext)

	// ExitPartTypeExpr is called when exiting the partTypeExpr production.
	ExitPartTypeExpr(c *PartTypeExprContext)

	// ExitDescStatement is called when exiting the descStatement production.
	ExitDescStatement(c *DescStatementContext)

	// ExitAnalyzeStatement is called when exiting the analyzeStatement production.
	ExitAnalyzeStatement(c *AnalyzeStatementContext)

	// ExitForColumnsStatement is called when exiting the forColumnsStatement production.
	ExitForColumnsStatement(c *ForColumnsStatementContext)

	// ExitColumnNameOrList is called when exiting the columnNameOrList production.
	ExitColumnNameOrList(c *ColumnNameOrListContext)

	// ExitShowStatement is called when exiting the showStatement production.
	ExitShowStatement(c *ShowStatementContext)

	// ExitListStatement is called when exiting the listStatement production.
	ExitListStatement(c *ListStatementContext)

	// ExitBareDate is called when exiting the bareDate production.
	ExitBareDate(c *BareDateContext)

	// ExitLockStatement is called when exiting the lockStatement production.
	ExitLockStatement(c *LockStatementContext)

	// ExitLockDatabase is called when exiting the lockDatabase production.
	ExitLockDatabase(c *LockDatabaseContext)

	// ExitLockMode is called when exiting the lockMode production.
	ExitLockMode(c *LockModeContext)

	// ExitUnlockStatement is called when exiting the unlockStatement production.
	ExitUnlockStatement(c *UnlockStatementContext)

	// ExitUnlockDatabase is called when exiting the unlockDatabase production.
	ExitUnlockDatabase(c *UnlockDatabaseContext)

	// ExitResourceList is called when exiting the resourceList production.
	ExitResourceList(c *ResourceListContext)

	// ExitResource is called when exiting the resource production.
	ExitResource(c *ResourceContext)

	// ExitResourceType is called when exiting the resourceType production.
	ExitResourceType(c *ResourceTypeContext)

	// ExitCreateFunctionStatement is called when exiting the createFunctionStatement production.
	ExitCreateFunctionStatement(c *CreateFunctionStatementContext)

	// ExitDropFunctionStatement is called when exiting the dropFunctionStatement production.
	ExitDropFunctionStatement(c *DropFunctionStatementContext)

	// ExitReloadFunctionStatement is called when exiting the reloadFunctionStatement production.
	ExitReloadFunctionStatement(c *ReloadFunctionStatementContext)

	// ExitCreateMacroStatement is called when exiting the createMacroStatement production.
	ExitCreateMacroStatement(c *CreateMacroStatementContext)

	// ExitDropMacroStatement is called when exiting the dropMacroStatement production.
	ExitDropMacroStatement(c *DropMacroStatementContext)

	// ExitCreateSqlFunctionStatement is called when exiting the createSqlFunctionStatement production.
	ExitCreateSqlFunctionStatement(c *CreateSqlFunctionStatementContext)

	// ExitCloneTableStatement is called when exiting the cloneTableStatement production.
	ExitCloneTableStatement(c *CloneTableStatementContext)

	// ExitCreateViewStatement is called when exiting the createViewStatement production.
	ExitCreateViewStatement(c *CreateViewStatementContext)

	// ExitViewPartition is called when exiting the viewPartition production.
	ExitViewPartition(c *ViewPartitionContext)

	// ExitDropViewStatement is called when exiting the dropViewStatement production.
	ExitDropViewStatement(c *DropViewStatementContext)

	// ExitCreateMaterializedViewStatement is called when exiting the createMaterializedViewStatement production.
	ExitCreateMaterializedViewStatement(c *CreateMaterializedViewStatementContext)

	// ExitDropMaterializedViewStatement is called when exiting the dropMaterializedViewStatement production.
	ExitDropMaterializedViewStatement(c *DropMaterializedViewStatementContext)

	// ExitShowFunctionIdentifier is called when exiting the showFunctionIdentifier production.
	ExitShowFunctionIdentifier(c *ShowFunctionIdentifierContext)

	// ExitShowStmtIdentifier is called when exiting the showStmtIdentifier production.
	ExitShowStmtIdentifier(c *ShowStmtIdentifierContext)

	// ExitTableComment is called when exiting the tableComment production.
	ExitTableComment(c *TableCommentContext)

	// ExitTablePartition is called when exiting the tablePartition production.
	ExitTablePartition(c *TablePartitionContext)

	// ExitTableBuckets is called when exiting the tableBuckets production.
	ExitTableBuckets(c *TableBucketsContext)

	// ExitTableShards is called when exiting the tableShards production.
	ExitTableShards(c *TableShardsContext)

	// ExitTableSkewed is called when exiting the tableSkewed production.
	ExitTableSkewed(c *TableSkewedContext)

	// ExitRowFormat is called when exiting the rowFormat production.
	ExitRowFormat(c *RowFormatContext)

	// ExitRecordReader is called when exiting the recordReader production.
	ExitRecordReader(c *RecordReaderContext)

	// ExitRecordWriter is called when exiting the recordWriter production.
	ExitRecordWriter(c *RecordWriterContext)

	// ExitRowFormatSerde is called when exiting the rowFormatSerde production.
	ExitRowFormatSerde(c *RowFormatSerdeContext)

	// ExitRowFormatDelimited is called when exiting the rowFormatDelimited production.
	ExitRowFormatDelimited(c *RowFormatDelimitedContext)

	// ExitTableRowFormat is called when exiting the tableRowFormat production.
	ExitTableRowFormat(c *TableRowFormatContext)

	// ExitTablePropertiesPrefixed is called when exiting the tablePropertiesPrefixed production.
	ExitTablePropertiesPrefixed(c *TablePropertiesPrefixedContext)

	// ExitTableProperties is called when exiting the tableProperties production.
	ExitTableProperties(c *TablePropertiesContext)

	// ExitTablePropertiesList is called when exiting the tablePropertiesList production.
	ExitTablePropertiesList(c *TablePropertiesListContext)

	// ExitKeyValueProperty is called when exiting the keyValueProperty production.
	ExitKeyValueProperty(c *KeyValuePropertyContext)

	// ExitUserDefinedJoinPropertiesList is called when exiting the userDefinedJoinPropertiesList production.
	ExitUserDefinedJoinPropertiesList(c *UserDefinedJoinPropertiesListContext)

	// ExitKeyPrivProperty is called when exiting the keyPrivProperty production.
	ExitKeyPrivProperty(c *KeyPrivPropertyContext)

	// ExitKeyProperty is called when exiting the keyProperty production.
	ExitKeyProperty(c *KeyPropertyContext)

	// ExitTableRowFormatFieldIdentifier is called when exiting the tableRowFormatFieldIdentifier production.
	ExitTableRowFormatFieldIdentifier(c *TableRowFormatFieldIdentifierContext)

	// ExitTableRowFormatCollItemsIdentifier is called when exiting the tableRowFormatCollItemsIdentifier production.
	ExitTableRowFormatCollItemsIdentifier(c *TableRowFormatCollItemsIdentifierContext)

	// ExitTableRowFormatMapKeysIdentifier is called when exiting the tableRowFormatMapKeysIdentifier production.
	ExitTableRowFormatMapKeysIdentifier(c *TableRowFormatMapKeysIdentifierContext)

	// ExitTableRowFormatLinesIdentifier is called when exiting the tableRowFormatLinesIdentifier production.
	ExitTableRowFormatLinesIdentifier(c *TableRowFormatLinesIdentifierContext)

	// ExitTableRowNullFormat is called when exiting the tableRowNullFormat production.
	ExitTableRowNullFormat(c *TableRowNullFormatContext)

	// ExitTableFileFormat is called when exiting the tableFileFormat production.
	ExitTableFileFormat(c *TableFileFormatContext)

	// ExitTableLocation is called when exiting the tableLocation production.
	ExitTableLocation(c *TableLocationContext)

	// ExitExternalTableResource is called when exiting the externalTableResource production.
	ExitExternalTableResource(c *ExternalTableResourceContext)

	// ExitViewResource is called when exiting the viewResource production.
	ExitViewResource(c *ViewResourceContext)

	// ExitOutOfLineConstraints is called when exiting the outOfLineConstraints production.
	ExitOutOfLineConstraints(c *OutOfLineConstraintsContext)

	// ExitEnableSpec is called when exiting the enableSpec production.
	ExitEnableSpec(c *EnableSpecContext)

	// ExitValidateSpec is called when exiting the validateSpec production.
	ExitValidateSpec(c *ValidateSpecContext)

	// ExitRelySpec is called when exiting the relySpec production.
	ExitRelySpec(c *RelySpecContext)

	// ExitColumnNameTypeConstraintList is called when exiting the columnNameTypeConstraintList production.
	ExitColumnNameTypeConstraintList(c *ColumnNameTypeConstraintListContext)

	// ExitColumnNameTypeList is called when exiting the columnNameTypeList production.
	ExitColumnNameTypeList(c *ColumnNameTypeListContext)

	// ExitPartitionColumnNameTypeList is called when exiting the partitionColumnNameTypeList production.
	ExitPartitionColumnNameTypeList(c *PartitionColumnNameTypeListContext)

	// ExitColumnNameTypeConstraintWithPosList is called when exiting the columnNameTypeConstraintWithPosList production.
	ExitColumnNameTypeConstraintWithPosList(c *ColumnNameTypeConstraintWithPosListContext)

	// ExitColumnNameColonTypeList is called when exiting the columnNameColonTypeList production.
	ExitColumnNameColonTypeList(c *ColumnNameColonTypeListContext)

	// ExitColumnNameList is called when exiting the columnNameList production.
	ExitColumnNameList(c *ColumnNameListContext)

	// ExitColumnNameListInParentheses is called when exiting the columnNameListInParentheses production.
	ExitColumnNameListInParentheses(c *ColumnNameListInParenthesesContext)

	// ExitColumnName is called when exiting the columnName production.
	ExitColumnName(c *ColumnNameContext)

	// ExitColumnNameOrderList is called when exiting the columnNameOrderList production.
	ExitColumnNameOrderList(c *ColumnNameOrderListContext)

	// ExitClusterColumnNameOrderList is called when exiting the clusterColumnNameOrderList production.
	ExitClusterColumnNameOrderList(c *ClusterColumnNameOrderListContext)

	// ExitSkewedValueElement is called when exiting the skewedValueElement production.
	ExitSkewedValueElement(c *SkewedValueElementContext)

	// ExitSkewedColumnValuePairList is called when exiting the skewedColumnValuePairList production.
	ExitSkewedColumnValuePairList(c *SkewedColumnValuePairListContext)

	// ExitSkewedColumnValuePair is called when exiting the skewedColumnValuePair production.
	ExitSkewedColumnValuePair(c *SkewedColumnValuePairContext)

	// ExitSkewedColumnValues is called when exiting the skewedColumnValues production.
	ExitSkewedColumnValues(c *SkewedColumnValuesContext)

	// ExitSkewedColumnValue is called when exiting the skewedColumnValue production.
	ExitSkewedColumnValue(c *SkewedColumnValueContext)

	// ExitSkewedValueLocationElement is called when exiting the skewedValueLocationElement production.
	ExitSkewedValueLocationElement(c *SkewedValueLocationElementContext)

	// ExitColumnNameOrder is called when exiting the columnNameOrder production.
	ExitColumnNameOrder(c *ColumnNameOrderContext)

	// ExitColumnNameCommentList is called when exiting the columnNameCommentList production.
	ExitColumnNameCommentList(c *ColumnNameCommentListContext)

	// ExitColumnNameComment is called when exiting the columnNameComment production.
	ExitColumnNameComment(c *ColumnNameCommentContext)

	// ExitColumnRefOrder is called when exiting the columnRefOrder production.
	ExitColumnRefOrder(c *ColumnRefOrderContext)

	// ExitColumnNameTypeConstraint is called when exiting the columnNameTypeConstraint production.
	ExitColumnNameTypeConstraint(c *ColumnNameTypeConstraintContext)

	// ExitColumnNameType is called when exiting the columnNameType production.
	ExitColumnNameType(c *ColumnNameTypeContext)

	// ExitPartitionColumnNameType is called when exiting the partitionColumnNameType production.
	ExitPartitionColumnNameType(c *PartitionColumnNameTypeContext)

	// ExitMultipartIdentifier is called when exiting the multipartIdentifier production.
	ExitMultipartIdentifier(c *MultipartIdentifierContext)

	// ExitColumnNameTypeConstraintWithPos is called when exiting the columnNameTypeConstraintWithPos production.
	ExitColumnNameTypeConstraintWithPos(c *ColumnNameTypeConstraintWithPosContext)

	// ExitConstraints is called when exiting the constraints production.
	ExitConstraints(c *ConstraintsContext)

	// ExitPrimaryKey is called when exiting the primaryKey production.
	ExitPrimaryKey(c *PrimaryKeyContext)

	// ExitNullableSpec is called when exiting the nullableSpec production.
	ExitNullableSpec(c *NullableSpecContext)

	// ExitDefaultValue is called when exiting the defaultValue production.
	ExitDefaultValue(c *DefaultValueContext)

	// ExitColumnNameColonType is called when exiting the columnNameColonType production.
	ExitColumnNameColonType(c *ColumnNameColonTypeContext)

	// ExitColType is called when exiting the colType production.
	ExitColType(c *ColTypeContext)

	// ExitColTypeList is called when exiting the colTypeList production.
	ExitColTypeList(c *ColTypeListContext)

	// ExitAnyType is called when exiting the anyType production.
	ExitAnyType(c *AnyTypeContext)

	// ExitAnyTypeList is called when exiting the anyTypeList production.
	ExitAnyTypeList(c *AnyTypeListContext)

	// ExitTableTypeInfo is called when exiting the tableTypeInfo production.
	ExitTableTypeInfo(c *TableTypeInfoContext)

	// ExitType is called when exiting the type production.
	ExitType(c *TypeContext)

	// ExitPrimitiveType is called when exiting the primitiveType production.
	ExitPrimitiveType(c *PrimitiveTypeContext)

	// ExitBuiltinTypeOrUdt is called when exiting the builtinTypeOrUdt production.
	ExitBuiltinTypeOrUdt(c *BuiltinTypeOrUdtContext)

	// ExitPrimitiveTypeOrUdt is called when exiting the primitiveTypeOrUdt production.
	ExitPrimitiveTypeOrUdt(c *PrimitiveTypeOrUdtContext)

	// ExitListType is called when exiting the listType production.
	ExitListType(c *ListTypeContext)

	// ExitStructType is called when exiting the structType production.
	ExitStructType(c *StructTypeContext)

	// ExitMapType is called when exiting the mapType production.
	ExitMapType(c *MapTypeContext)

	// ExitUnionType is called when exiting the unionType production.
	ExitUnionType(c *UnionTypeContext)

	// ExitSetOperator is called when exiting the setOperator production.
	ExitSetOperator(c *SetOperatorContext)

	// ExitWithClause is called when exiting the withClause production.
	ExitWithClause(c *WithClauseContext)

	// ExitInsertClause is called when exiting the insertClause production.
	ExitInsertClause(c *InsertClauseContext)

	// ExitDestination is called when exiting the destination production.
	ExitDestination(c *DestinationContext)

	// ExitDeleteStatement is called when exiting the deleteStatement production.
	ExitDeleteStatement(c *DeleteStatementContext)

	// ExitColumnAssignmentClause is called when exiting the columnAssignmentClause production.
	ExitColumnAssignmentClause(c *ColumnAssignmentClauseContext)

	// ExitSetColumnsClause is called when exiting the setColumnsClause production.
	ExitSetColumnsClause(c *SetColumnsClauseContext)

	// ExitUpdateStatement is called when exiting the updateStatement production.
	ExitUpdateStatement(c *UpdateStatementContext)

	// ExitMergeStatement is called when exiting the mergeStatement production.
	ExitMergeStatement(c *MergeStatementContext)

	// ExitMergeTargetTable is called when exiting the mergeTargetTable production.
	ExitMergeTargetTable(c *MergeTargetTableContext)

	// ExitMergeSourceTable is called when exiting the mergeSourceTable production.
	ExitMergeSourceTable(c *MergeSourceTableContext)

	// ExitMergeAction is called when exiting the mergeAction production.
	ExitMergeAction(c *MergeActionContext)

	// ExitMergeValuesCaluse is called when exiting the mergeValuesCaluse production.
	ExitMergeValuesCaluse(c *MergeValuesCaluseContext)

	// ExitMergeSetColumnsClause is called when exiting the mergeSetColumnsClause production.
	ExitMergeSetColumnsClause(c *MergeSetColumnsClauseContext)

	// ExitMergeColumnAssignmentClause is called when exiting the mergeColumnAssignmentClause production.
	ExitMergeColumnAssignmentClause(c *MergeColumnAssignmentClauseContext)

	// ExitSelectClause is called when exiting the selectClause production.
	ExitSelectClause(c *SelectClauseContext)

	// ExitSelectList is called when exiting the selectList production.
	ExitSelectList(c *SelectListContext)

	// ExitSelectTrfmClause is called when exiting the selectTrfmClause production.
	ExitSelectTrfmClause(c *SelectTrfmClauseContext)

	// ExitHintClause is called when exiting the hintClause production.
	ExitHintClause(c *HintClauseContext)

	// ExitHintList is called when exiting the hintList production.
	ExitHintList(c *HintListContext)

	// ExitHintItem is called when exiting the hintItem production.
	ExitHintItem(c *HintItemContext)

	// ExitDynamicfilterHint is called when exiting the dynamicfilterHint production.
	ExitDynamicfilterHint(c *DynamicfilterHintContext)

	// ExitMapJoinHint is called when exiting the mapJoinHint production.
	ExitMapJoinHint(c *MapJoinHintContext)

	// ExitSkewJoinHint is called when exiting the skewJoinHint production.
	ExitSkewJoinHint(c *SkewJoinHintContext)

	// ExitSelectivityHint is called when exiting the selectivityHint production.
	ExitSelectivityHint(c *SelectivityHintContext)

	// ExitMultipleSkewHintArgs is called when exiting the multipleSkewHintArgs production.
	ExitMultipleSkewHintArgs(c *MultipleSkewHintArgsContext)

	// ExitSkewJoinHintArgs is called when exiting the skewJoinHintArgs production.
	ExitSkewJoinHintArgs(c *SkewJoinHintArgsContext)

	// ExitSkewColumns is called when exiting the skewColumns production.
	ExitSkewColumns(c *SkewColumnsContext)

	// ExitSkewJoinHintKeyValues is called when exiting the skewJoinHintKeyValues production.
	ExitSkewJoinHintKeyValues(c *SkewJoinHintKeyValuesContext)

	// ExitHintName is called when exiting the hintName production.
	ExitHintName(c *HintNameContext)

	// ExitHintArgs is called when exiting the hintArgs production.
	ExitHintArgs(c *HintArgsContext)

	// ExitHintArgName is called when exiting the hintArgName production.
	ExitHintArgName(c *HintArgNameContext)

	// ExitSelectItem is called when exiting the selectItem production.
	ExitSelectItem(c *SelectItemContext)

	// ExitTrfmClause is called when exiting the trfmClause production.
	ExitTrfmClause(c *TrfmClauseContext)

	// ExitSelectExpression is called when exiting the selectExpression production.
	ExitSelectExpression(c *SelectExpressionContext)

	// ExitSelectExpressionList is called when exiting the selectExpressionList production.
	ExitSelectExpressionList(c *SelectExpressionListContext)

	// ExitWindow_clause is called when exiting the window_clause production.
	ExitWindow_clause(c *Window_clauseContext)

	// ExitWindow_defn is called when exiting the window_defn production.
	ExitWindow_defn(c *Window_defnContext)

	// ExitWindow_specification is called when exiting the window_specification production.
	ExitWindow_specification(c *Window_specificationContext)

	// ExitWindow_frame is called when exiting the window_frame production.
	ExitWindow_frame(c *Window_frameContext)

	// ExitFrame_exclusion is called when exiting the frame_exclusion production.
	ExitFrame_exclusion(c *Frame_exclusionContext)

	// ExitWindow_frame_start_boundary is called when exiting the window_frame_start_boundary production.
	ExitWindow_frame_start_boundary(c *Window_frame_start_boundaryContext)

	// ExitWindow_frame_boundary is called when exiting the window_frame_boundary production.
	ExitWindow_frame_boundary(c *Window_frame_boundaryContext)

	// ExitTableAllColumns is called when exiting the tableAllColumns production.
	ExitTableAllColumns(c *TableAllColumnsContext)

	// ExitTableOrColumn is called when exiting the tableOrColumn production.
	ExitTableOrColumn(c *TableOrColumnContext)

	// ExitTableAndColumnRef is called when exiting the tableAndColumnRef production.
	ExitTableAndColumnRef(c *TableAndColumnRefContext)

	// ExitExpressionList is called when exiting the expressionList production.
	ExitExpressionList(c *ExpressionListContext)

	// ExitAliasList is called when exiting the aliasList production.
	ExitAliasList(c *AliasListContext)

	// ExitFromClause is called when exiting the fromClause production.
	ExitFromClause(c *FromClauseContext)

	// ExitJoinSource is called when exiting the joinSource production.
	ExitJoinSource(c *JoinSourceContext)

	// ExitJoinRHS is called when exiting the joinRHS production.
	ExitJoinRHS(c *JoinRHSContext)

	// ExitUniqueJoinSource is called when exiting the uniqueJoinSource production.
	ExitUniqueJoinSource(c *UniqueJoinSourceContext)

	// ExitUniqueJoinExpr is called when exiting the uniqueJoinExpr production.
	ExitUniqueJoinExpr(c *UniqueJoinExprContext)

	// ExitUniqueJoinToken is called when exiting the uniqueJoinToken production.
	ExitUniqueJoinToken(c *UniqueJoinTokenContext)

	// ExitJoinToken is called when exiting the joinToken production.
	ExitJoinToken(c *JoinTokenContext)

	// ExitLateralView is called when exiting the lateralView production.
	ExitLateralView(c *LateralViewContext)

	// ExitTableAlias is called when exiting the tableAlias production.
	ExitTableAlias(c *TableAliasContext)

	// ExitTableBucketSample is called when exiting the tableBucketSample production.
	ExitTableBucketSample(c *TableBucketSampleContext)

	// ExitSplitSample is called when exiting the splitSample production.
	ExitSplitSample(c *SplitSampleContext)

	// ExitTableSample is called when exiting the tableSample production.
	ExitTableSample(c *TableSampleContext)

	// ExitTableSource is called when exiting the tableSource production.
	ExitTableSource(c *TableSourceContext)

	// ExitAvailableSql11KeywordsForOdpsTableAlias is called when exiting the availableSql11KeywordsForOdpsTableAlias production.
	ExitAvailableSql11KeywordsForOdpsTableAlias(c *AvailableSql11KeywordsForOdpsTableAliasContext)

	// ExitTableName is called when exiting the tableName production.
	ExitTableName(c *TableNameContext)

	// ExitPartitioningSpec is called when exiting the partitioningSpec production.
	ExitPartitioningSpec(c *PartitioningSpecContext)

	// ExitPartitionTableFunctionSource is called when exiting the partitionTableFunctionSource production.
	ExitPartitionTableFunctionSource(c *PartitionTableFunctionSourceContext)

	// ExitPartitionedTableFunction is called when exiting the partitionedTableFunction production.
	ExitPartitionedTableFunction(c *PartitionedTableFunctionContext)

	// ExitWhereClause is called when exiting the whereClause production.
	ExitWhereClause(c *WhereClauseContext)

	// ExitValueRowConstructor is called when exiting the valueRowConstructor production.
	ExitValueRowConstructor(c *ValueRowConstructorContext)

	// ExitValuesTableConstructor is called when exiting the valuesTableConstructor production.
	ExitValuesTableConstructor(c *ValuesTableConstructorContext)

	// ExitValuesClause is called when exiting the valuesClause production.
	ExitValuesClause(c *ValuesClauseContext)

	// ExitVirtualTableSource is called when exiting the virtualTableSource production.
	ExitVirtualTableSource(c *VirtualTableSourceContext)

	// ExitTableNameColList is called when exiting the tableNameColList production.
	ExitTableNameColList(c *TableNameColListContext)

	// ExitFunctionTypeCubeOrRollup is called when exiting the functionTypeCubeOrRollup production.
	ExitFunctionTypeCubeOrRollup(c *FunctionTypeCubeOrRollupContext)

	// ExitGroupingSetsItem is called when exiting the groupingSetsItem production.
	ExitGroupingSetsItem(c *GroupingSetsItemContext)

	// ExitGroupingSetsClause is called when exiting the groupingSetsClause production.
	ExitGroupingSetsClause(c *GroupingSetsClauseContext)

	// ExitGroupByKey is called when exiting the groupByKey production.
	ExitGroupByKey(c *GroupByKeyContext)

	// ExitGroupByClause is called when exiting the groupByClause production.
	ExitGroupByClause(c *GroupByClauseContext)

	// ExitGroupingSetExpression is called when exiting the groupingSetExpression production.
	ExitGroupingSetExpression(c *GroupingSetExpressionContext)

	// ExitGroupingSetExpressionMultiple is called when exiting the groupingSetExpressionMultiple production.
	ExitGroupingSetExpressionMultiple(c *GroupingSetExpressionMultipleContext)

	// ExitGroupingExpressionSingle is called when exiting the groupingExpressionSingle production.
	ExitGroupingExpressionSingle(c *GroupingExpressionSingleContext)

	// ExitHavingClause is called when exiting the havingClause production.
	ExitHavingClause(c *HavingClauseContext)

	// ExitHavingCondition is called when exiting the havingCondition production.
	ExitHavingCondition(c *HavingConditionContext)

	// ExitExpressionsInParenthese is called when exiting the expressionsInParenthese production.
	ExitExpressionsInParenthese(c *ExpressionsInParentheseContext)

	// ExitExpressionsNotInParenthese is called when exiting the expressionsNotInParenthese production.
	ExitExpressionsNotInParenthese(c *ExpressionsNotInParentheseContext)

	// ExitColumnRefOrderInParenthese is called when exiting the columnRefOrderInParenthese production.
	ExitColumnRefOrderInParenthese(c *ColumnRefOrderInParentheseContext)

	// ExitColumnRefOrderNotInParenthese is called when exiting the columnRefOrderNotInParenthese production.
	ExitColumnRefOrderNotInParenthese(c *ColumnRefOrderNotInParentheseContext)

	// ExitOrderByClause is called when exiting the orderByClause production.
	ExitOrderByClause(c *OrderByClauseContext)

	// ExitColumnNameOrIndexInParenthese is called when exiting the columnNameOrIndexInParenthese production.
	ExitColumnNameOrIndexInParenthese(c *ColumnNameOrIndexInParentheseContext)

	// ExitColumnNameOrIndexNotInParenthese is called when exiting the columnNameOrIndexNotInParenthese production.
	ExitColumnNameOrIndexNotInParenthese(c *ColumnNameOrIndexNotInParentheseContext)

	// ExitColumnNameOrIndex is called when exiting the columnNameOrIndex production.
	ExitColumnNameOrIndex(c *ColumnNameOrIndexContext)

	// ExitZorderByClause is called when exiting the zorderByClause production.
	ExitZorderByClause(c *ZorderByClauseContext)

	// ExitClusterByClause is called when exiting the clusterByClause production.
	ExitClusterByClause(c *ClusterByClauseContext)

	// ExitPartitionByClause is called when exiting the partitionByClause production.
	ExitPartitionByClause(c *PartitionByClauseContext)

	// ExitDistributeByClause is called when exiting the distributeByClause production.
	ExitDistributeByClause(c *DistributeByClauseContext)

	// ExitSortByClause is called when exiting the sortByClause production.
	ExitSortByClause(c *SortByClauseContext)

	// ExitFunction is called when exiting the function production.
	ExitFunction(c *FunctionContext)

	// ExitFunctionArgument is called when exiting the functionArgument production.
	ExitFunctionArgument(c *FunctionArgumentContext)

	// ExitBuiltinFunctionStructure is called when exiting the builtinFunctionStructure production.
	ExitBuiltinFunctionStructure(c *BuiltinFunctionStructureContext)

	// ExitFunctionName is called when exiting the functionName production.
	ExitFunctionName(c *FunctionNameContext)

	// ExitCastExpression is called when exiting the castExpression production.
	ExitCastExpression(c *CastExpressionContext)

	// ExitCaseExpression is called when exiting the caseExpression production.
	ExitCaseExpression(c *CaseExpressionContext)

	// ExitWhenExpression is called when exiting the whenExpression production.
	ExitWhenExpression(c *WhenExpressionContext)

	// ExitConstant is called when exiting the constant production.
	ExitConstant(c *ConstantContext)

	// ExitSimpleStringLiteral is called when exiting the simpleStringLiteral production.
	ExitSimpleStringLiteral(c *SimpleStringLiteralContext)

	// ExitStringLiteral is called when exiting the stringLiteral production.
	ExitStringLiteral(c *StringLiteralContext)

	// ExitDoubleQuoteStringLiteral is called when exiting the doubleQuoteStringLiteral production.
	ExitDoubleQuoteStringLiteral(c *DoubleQuoteStringLiteralContext)

	// ExitCharSetStringLiteral is called when exiting the charSetStringLiteral production.
	ExitCharSetStringLiteral(c *CharSetStringLiteralContext)

	// ExitDateLiteral is called when exiting the dateLiteral production.
	ExitDateLiteral(c *DateLiteralContext)

	// ExitDateTimeLiteral is called when exiting the dateTimeLiteral production.
	ExitDateTimeLiteral(c *DateTimeLiteralContext)

	// ExitTimestampLiteral is called when exiting the timestampLiteral production.
	ExitTimestampLiteral(c *TimestampLiteralContext)

	// ExitIntervalLiteral is called when exiting the intervalLiteral production.
	ExitIntervalLiteral(c *IntervalLiteralContext)

	// ExitIntervalQualifiers is called when exiting the intervalQualifiers production.
	ExitIntervalQualifiers(c *IntervalQualifiersContext)

	// ExitIntervalQualifiersUnit is called when exiting the intervalQualifiersUnit production.
	ExitIntervalQualifiersUnit(c *IntervalQualifiersUnitContext)

	// ExitIntervalQualifierPrecision is called when exiting the intervalQualifierPrecision production.
	ExitIntervalQualifierPrecision(c *IntervalQualifierPrecisionContext)

	// ExitBooleanValue is called when exiting the booleanValue production.
	ExitBooleanValue(c *BooleanValueContext)

	// ExitTableOrPartition is called when exiting the tableOrPartition production.
	ExitTableOrPartition(c *TableOrPartitionContext)

	// ExitPartitionSpec is called when exiting the partitionSpec production.
	ExitPartitionSpec(c *PartitionSpecContext)

	// ExitPartitionVal is called when exiting the partitionVal production.
	ExitPartitionVal(c *PartitionValContext)

	// ExitDateWithoutQuote is called when exiting the dateWithoutQuote production.
	ExitDateWithoutQuote(c *DateWithoutQuoteContext)

	// ExitDropPartitionSpec is called when exiting the dropPartitionSpec production.
	ExitDropPartitionSpec(c *DropPartitionSpecContext)

	// ExitSysFuncNames is called when exiting the sysFuncNames production.
	ExitSysFuncNames(c *SysFuncNamesContext)

	// ExitDescFuncNames is called when exiting the descFuncNames production.
	ExitDescFuncNames(c *DescFuncNamesContext)

	// ExitFunctionIdentifier is called when exiting the functionIdentifier production.
	ExitFunctionIdentifier(c *FunctionIdentifierContext)

	// ExitReserved is called when exiting the reserved production.
	ExitReserved(c *ReservedContext)

	// ExitNonReserved is called when exiting the nonReserved production.
	ExitNonReserved(c *NonReservedContext)

	// ExitSql11ReservedKeywordsUsedAsCastFunctionName is called when exiting the sql11ReservedKeywordsUsedAsCastFunctionName production.
	ExitSql11ReservedKeywordsUsedAsCastFunctionName(c *Sql11ReservedKeywordsUsedAsCastFunctionNameContext)

	// ExitSql11ReservedKeywordsUsedAsIdentifier is called when exiting the sql11ReservedKeywordsUsedAsIdentifier production.
	ExitSql11ReservedKeywordsUsedAsIdentifier(c *Sql11ReservedKeywordsUsedAsIdentifierContext)
}
