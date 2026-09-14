// Code generated from grammar/OdpsParser.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // OdpsParser
import "github.com/antlr4-go/antlr/v4"

// BaseOdpsParserListener is a complete listener for a parse tree produced by OdpsParser.
type BaseOdpsParserListener struct{}

var _ OdpsParserListener = &BaseOdpsParserListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseOdpsParserListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseOdpsParserListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseOdpsParserListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseOdpsParserListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterScript is called when production script is entered.
func (s *BaseOdpsParserListener) EnterScript(ctx *ScriptContext) {}

// ExitScript is called when production script is exited.
func (s *BaseOdpsParserListener) ExitScript(ctx *ScriptContext) {}

// EnterUserCodeBlock is called when production userCodeBlock is entered.
func (s *BaseOdpsParserListener) EnterUserCodeBlock(ctx *UserCodeBlockContext) {}

// ExitUserCodeBlock is called when production userCodeBlock is exited.
func (s *BaseOdpsParserListener) ExitUserCodeBlock(ctx *UserCodeBlockContext) {}

// EnterStatement is called when production statement is entered.
func (s *BaseOdpsParserListener) EnterStatement(ctx *StatementContext) {}

// ExitStatement is called when production statement is exited.
func (s *BaseOdpsParserListener) ExitStatement(ctx *StatementContext) {}

// EnterCompoundStatement is called when production compoundStatement is entered.
func (s *BaseOdpsParserListener) EnterCompoundStatement(ctx *CompoundStatementContext) {}

// ExitCompoundStatement is called when production compoundStatement is exited.
func (s *BaseOdpsParserListener) ExitCompoundStatement(ctx *CompoundStatementContext) {}

// EnterNoopStatement is called when production noopStatement is entered.
func (s *BaseOdpsParserListener) EnterNoopStatement(ctx *NoopStatementContext) {}

// ExitNoopStatement is called when production noopStatement is exited.
func (s *BaseOdpsParserListener) ExitNoopStatement(ctx *NoopStatementContext) {}

// EnterExecStatement is called when production execStatement is entered.
func (s *BaseOdpsParserListener) EnterExecStatement(ctx *ExecStatementContext) {}

// ExitExecStatement is called when production execStatement is exited.
func (s *BaseOdpsParserListener) ExitExecStatement(ctx *ExecStatementContext) {}

// EnterCteStatement is called when production cteStatement is entered.
func (s *BaseOdpsParserListener) EnterCteStatement(ctx *CteStatementContext) {}

// ExitCteStatement is called when production cteStatement is exited.
func (s *BaseOdpsParserListener) ExitCteStatement(ctx *CteStatementContext) {}

// EnterTableAliasWithCols is called when production tableAliasWithCols is entered.
func (s *BaseOdpsParserListener) EnterTableAliasWithCols(ctx *TableAliasWithColsContext) {}

// ExitTableAliasWithCols is called when production tableAliasWithCols is exited.
func (s *BaseOdpsParserListener) ExitTableAliasWithCols(ctx *TableAliasWithColsContext) {}

// EnterSubQuerySource is called when production subQuerySource is entered.
func (s *BaseOdpsParserListener) EnterSubQuerySource(ctx *SubQuerySourceContext) {}

// ExitSubQuerySource is called when production subQuerySource is exited.
func (s *BaseOdpsParserListener) ExitSubQuerySource(ctx *SubQuerySourceContext) {}

// EnterExplainStatement is called when production explainStatement is entered.
func (s *BaseOdpsParserListener) EnterExplainStatement(ctx *ExplainStatementContext) {}

// ExitExplainStatement is called when production explainStatement is exited.
func (s *BaseOdpsParserListener) ExitExplainStatement(ctx *ExplainStatementContext) {}

// EnterIfStatement is called when production ifStatement is entered.
func (s *BaseOdpsParserListener) EnterIfStatement(ctx *IfStatementContext) {}

// ExitIfStatement is called when production ifStatement is exited.
func (s *BaseOdpsParserListener) ExitIfStatement(ctx *IfStatementContext) {}

// EnterLoopStatement is called when production loopStatement is entered.
func (s *BaseOdpsParserListener) EnterLoopStatement(ctx *LoopStatementContext) {}

// ExitLoopStatement is called when production loopStatement is exited.
func (s *BaseOdpsParserListener) ExitLoopStatement(ctx *LoopStatementContext) {}

// EnterFunctionDefinition is called when production functionDefinition is entered.
func (s *BaseOdpsParserListener) EnterFunctionDefinition(ctx *FunctionDefinitionContext) {}

// ExitFunctionDefinition is called when production functionDefinition is exited.
func (s *BaseOdpsParserListener) ExitFunctionDefinition(ctx *FunctionDefinitionContext) {}

// EnterFunctionParameters is called when production functionParameters is entered.
func (s *BaseOdpsParserListener) EnterFunctionParameters(ctx *FunctionParametersContext) {}

// ExitFunctionParameters is called when production functionParameters is exited.
func (s *BaseOdpsParserListener) ExitFunctionParameters(ctx *FunctionParametersContext) {}

// EnterParameterDefinition is called when production parameterDefinition is entered.
func (s *BaseOdpsParserListener) EnterParameterDefinition(ctx *ParameterDefinitionContext) {}

// ExitParameterDefinition is called when production parameterDefinition is exited.
func (s *BaseOdpsParserListener) ExitParameterDefinition(ctx *ParameterDefinitionContext) {}

// EnterTypeDeclaration is called when production typeDeclaration is entered.
func (s *BaseOdpsParserListener) EnterTypeDeclaration(ctx *TypeDeclarationContext) {}

// ExitTypeDeclaration is called when production typeDeclaration is exited.
func (s *BaseOdpsParserListener) ExitTypeDeclaration(ctx *TypeDeclarationContext) {}

// EnterParameterTypeDeclaration is called when production parameterTypeDeclaration is entered.
func (s *BaseOdpsParserListener) EnterParameterTypeDeclaration(ctx *ParameterTypeDeclarationContext) {
}

// ExitParameterTypeDeclaration is called when production parameterTypeDeclaration is exited.
func (s *BaseOdpsParserListener) ExitParameterTypeDeclaration(ctx *ParameterTypeDeclarationContext) {}

// EnterFunctionTypeDeclaration is called when production functionTypeDeclaration is entered.
func (s *BaseOdpsParserListener) EnterFunctionTypeDeclaration(ctx *FunctionTypeDeclarationContext) {}

// ExitFunctionTypeDeclaration is called when production functionTypeDeclaration is exited.
func (s *BaseOdpsParserListener) ExitFunctionTypeDeclaration(ctx *FunctionTypeDeclarationContext) {}

// EnterParameterTypeDeclarationList is called when production parameterTypeDeclarationList is entered.
func (s *BaseOdpsParserListener) EnterParameterTypeDeclarationList(ctx *ParameterTypeDeclarationListContext) {
}

// ExitParameterTypeDeclarationList is called when production parameterTypeDeclarationList is exited.
func (s *BaseOdpsParserListener) ExitParameterTypeDeclarationList(ctx *ParameterTypeDeclarationListContext) {
}

// EnterParameterColumnNameTypeList is called when production parameterColumnNameTypeList is entered.
func (s *BaseOdpsParserListener) EnterParameterColumnNameTypeList(ctx *ParameterColumnNameTypeListContext) {
}

// ExitParameterColumnNameTypeList is called when production parameterColumnNameTypeList is exited.
func (s *BaseOdpsParserListener) ExitParameterColumnNameTypeList(ctx *ParameterColumnNameTypeListContext) {
}

// EnterParameterColumnNameType is called when production parameterColumnNameType is entered.
func (s *BaseOdpsParserListener) EnterParameterColumnNameType(ctx *ParameterColumnNameTypeContext) {}

// ExitParameterColumnNameType is called when production parameterColumnNameType is exited.
func (s *BaseOdpsParserListener) ExitParameterColumnNameType(ctx *ParameterColumnNameTypeContext) {}

// EnterVarSizeParam is called when production varSizeParam is entered.
func (s *BaseOdpsParserListener) EnterVarSizeParam(ctx *VarSizeParamContext) {}

// ExitVarSizeParam is called when production varSizeParam is exited.
func (s *BaseOdpsParserListener) ExitVarSizeParam(ctx *VarSizeParamContext) {}

// EnterAssignStatement is called when production assignStatement is entered.
func (s *BaseOdpsParserListener) EnterAssignStatement(ctx *AssignStatementContext) {}

// ExitAssignStatement is called when production assignStatement is exited.
func (s *BaseOdpsParserListener) ExitAssignStatement(ctx *AssignStatementContext) {}

// EnterPreSelectClauses is called when production preSelectClauses is entered.
func (s *BaseOdpsParserListener) EnterPreSelectClauses(ctx *PreSelectClausesContext) {}

// ExitPreSelectClauses is called when production preSelectClauses is exited.
func (s *BaseOdpsParserListener) ExitPreSelectClauses(ctx *PreSelectClausesContext) {}

// EnterPostSelectClauses is called when production postSelectClauses is entered.
func (s *BaseOdpsParserListener) EnterPostSelectClauses(ctx *PostSelectClausesContext) {}

// ExitPostSelectClauses is called when production postSelectClauses is exited.
func (s *BaseOdpsParserListener) ExitPostSelectClauses(ctx *PostSelectClausesContext) {}

// EnterSelectRest is called when production selectRest is entered.
func (s *BaseOdpsParserListener) EnterSelectRest(ctx *SelectRestContext) {}

// ExitSelectRest is called when production selectRest is exited.
func (s *BaseOdpsParserListener) ExitSelectRest(ctx *SelectRestContext) {}

// EnterMultiInsertFromRest is called when production multiInsertFromRest is entered.
func (s *BaseOdpsParserListener) EnterMultiInsertFromRest(ctx *MultiInsertFromRestContext) {}

// ExitMultiInsertFromRest is called when production multiInsertFromRest is exited.
func (s *BaseOdpsParserListener) ExitMultiInsertFromRest(ctx *MultiInsertFromRestContext) {}

// EnterFromRest is called when production fromRest is entered.
func (s *BaseOdpsParserListener) EnterFromRest(ctx *FromRestContext) {}

// ExitFromRest is called when production fromRest is exited.
func (s *BaseOdpsParserListener) ExitFromRest(ctx *FromRestContext) {}

// EnterSimpleQueryExpression is called when production simpleQueryExpression is entered.
func (s *BaseOdpsParserListener) EnterSimpleQueryExpression(ctx *SimpleQueryExpressionContext) {}

// ExitSimpleQueryExpression is called when production simpleQueryExpression is exited.
func (s *BaseOdpsParserListener) ExitSimpleQueryExpression(ctx *SimpleQueryExpressionContext) {}

// EnterSelectQueryExpression is called when production selectQueryExpression is entered.
func (s *BaseOdpsParserListener) EnterSelectQueryExpression(ctx *SelectQueryExpressionContext) {}

// ExitSelectQueryExpression is called when production selectQueryExpression is exited.
func (s *BaseOdpsParserListener) ExitSelectQueryExpression(ctx *SelectQueryExpressionContext) {}

// EnterFromQueryExpression is called when production fromQueryExpression is entered.
func (s *BaseOdpsParserListener) EnterFromQueryExpression(ctx *FromQueryExpressionContext) {}

// ExitFromQueryExpression is called when production fromQueryExpression is exited.
func (s *BaseOdpsParserListener) ExitFromQueryExpression(ctx *FromQueryExpressionContext) {}

// EnterSetOperationFactor is called when production setOperationFactor is entered.
func (s *BaseOdpsParserListener) EnterSetOperationFactor(ctx *SetOperationFactorContext) {}

// ExitSetOperationFactor is called when production setOperationFactor is exited.
func (s *BaseOdpsParserListener) ExitSetOperationFactor(ctx *SetOperationFactorContext) {}

// EnterQueryExpression is called when production queryExpression is entered.
func (s *BaseOdpsParserListener) EnterQueryExpression(ctx *QueryExpressionContext) {}

// ExitQueryExpression is called when production queryExpression is exited.
func (s *BaseOdpsParserListener) ExitQueryExpression(ctx *QueryExpressionContext) {}

// EnterQueryExpressionWithCTE is called when production queryExpressionWithCTE is entered.
func (s *BaseOdpsParserListener) EnterQueryExpressionWithCTE(ctx *QueryExpressionWithCTEContext) {}

// ExitQueryExpressionWithCTE is called when production queryExpressionWithCTE is exited.
func (s *BaseOdpsParserListener) ExitQueryExpressionWithCTE(ctx *QueryExpressionWithCTEContext) {}

// EnterSetRHS is called when production setRHS is entered.
func (s *BaseOdpsParserListener) EnterSetRHS(ctx *SetRHSContext) {}

// ExitSetRHS is called when production setRHS is exited.
func (s *BaseOdpsParserListener) ExitSetRHS(ctx *SetRHSContext) {}

// EnterMultiInsertSetOperationFactor is called when production multiInsertSetOperationFactor is entered.
func (s *BaseOdpsParserListener) EnterMultiInsertSetOperationFactor(ctx *MultiInsertSetOperationFactorContext) {
}

// ExitMultiInsertSetOperationFactor is called when production multiInsertSetOperationFactor is exited.
func (s *BaseOdpsParserListener) ExitMultiInsertSetOperationFactor(ctx *MultiInsertSetOperationFactorContext) {
}

// EnterMultiInsertSelect is called when production multiInsertSelect is entered.
func (s *BaseOdpsParserListener) EnterMultiInsertSelect(ctx *MultiInsertSelectContext) {}

// ExitMultiInsertSelect is called when production multiInsertSelect is exited.
func (s *BaseOdpsParserListener) ExitMultiInsertSelect(ctx *MultiInsertSelectContext) {}

// EnterMultiInsertSetRHS is called when production multiInsertSetRHS is entered.
func (s *BaseOdpsParserListener) EnterMultiInsertSetRHS(ctx *MultiInsertSetRHSContext) {}

// ExitMultiInsertSetRHS is called when production multiInsertSetRHS is exited.
func (s *BaseOdpsParserListener) ExitMultiInsertSetRHS(ctx *MultiInsertSetRHSContext) {}

// EnterMultiInsertBranch is called when production multiInsertBranch is entered.
func (s *BaseOdpsParserListener) EnterMultiInsertBranch(ctx *MultiInsertBranchContext) {}

// ExitMultiInsertBranch is called when production multiInsertBranch is exited.
func (s *BaseOdpsParserListener) ExitMultiInsertBranch(ctx *MultiInsertBranchContext) {}

// EnterFromStatement is called when production fromStatement is entered.
func (s *BaseOdpsParserListener) EnterFromStatement(ctx *FromStatementContext) {}

// ExitFromStatement is called when production fromStatement is exited.
func (s *BaseOdpsParserListener) ExitFromStatement(ctx *FromStatementContext) {}

// EnterInsertStatement is called when production insertStatement is entered.
func (s *BaseOdpsParserListener) EnterInsertStatement(ctx *InsertStatementContext) {}

// ExitInsertStatement is called when production insertStatement is exited.
func (s *BaseOdpsParserListener) ExitInsertStatement(ctx *InsertStatementContext) {}

// EnterSelectQueryStatement is called when production selectQueryStatement is entered.
func (s *BaseOdpsParserListener) EnterSelectQueryStatement(ctx *SelectQueryStatementContext) {}

// ExitSelectQueryStatement is called when production selectQueryStatement is exited.
func (s *BaseOdpsParserListener) ExitSelectQueryStatement(ctx *SelectQueryStatementContext) {}

// EnterQueryStatement is called when production queryStatement is entered.
func (s *BaseOdpsParserListener) EnterQueryStatement(ctx *QueryStatementContext) {}

// ExitQueryStatement is called when production queryStatement is exited.
func (s *BaseOdpsParserListener) ExitQueryStatement(ctx *QueryStatementContext) {}

// EnterInsertStatementWithCTE is called when production insertStatementWithCTE is entered.
func (s *BaseOdpsParserListener) EnterInsertStatementWithCTE(ctx *InsertStatementWithCTEContext) {}

// ExitInsertStatementWithCTE is called when production insertStatementWithCTE is exited.
func (s *BaseOdpsParserListener) ExitInsertStatementWithCTE(ctx *InsertStatementWithCTEContext) {}

// EnterSubQueryExpression is called when production subQueryExpression is entered.
func (s *BaseOdpsParserListener) EnterSubQueryExpression(ctx *SubQueryExpressionContext) {}

// ExitSubQueryExpression is called when production subQueryExpression is exited.
func (s *BaseOdpsParserListener) ExitSubQueryExpression(ctx *SubQueryExpressionContext) {}

// EnterLimitClause is called when production limitClause is entered.
func (s *BaseOdpsParserListener) EnterLimitClause(ctx *LimitClauseContext) {}

// ExitLimitClause is called when production limitClause is exited.
func (s *BaseOdpsParserListener) ExitLimitClause(ctx *LimitClauseContext) {}

// EnterFromSource is called when production fromSource is entered.
func (s *BaseOdpsParserListener) EnterFromSource(ctx *FromSourceContext) {}

// ExitFromSource is called when production fromSource is exited.
func (s *BaseOdpsParserListener) ExitFromSource(ctx *FromSourceContext) {}

// EnterTableVariableSource is called when production tableVariableSource is entered.
func (s *BaseOdpsParserListener) EnterTableVariableSource(ctx *TableVariableSourceContext) {}

// ExitTableVariableSource is called when production tableVariableSource is exited.
func (s *BaseOdpsParserListener) ExitTableVariableSource(ctx *TableVariableSourceContext) {}

// EnterTableFunctionSource is called when production tableFunctionSource is entered.
func (s *BaseOdpsParserListener) EnterTableFunctionSource(ctx *TableFunctionSourceContext) {}

// ExitTableFunctionSource is called when production tableFunctionSource is exited.
func (s *BaseOdpsParserListener) ExitTableFunctionSource(ctx *TableFunctionSourceContext) {}

// EnterCreateMachineLearningModelStatment is called when production createMachineLearningModelStatment is entered.
func (s *BaseOdpsParserListener) EnterCreateMachineLearningModelStatment(ctx *CreateMachineLearningModelStatmentContext) {
}

// ExitCreateMachineLearningModelStatment is called when production createMachineLearningModelStatment is exited.
func (s *BaseOdpsParserListener) ExitCreateMachineLearningModelStatment(ctx *CreateMachineLearningModelStatmentContext) {
}

// EnterVariableName is called when production variableName is entered.
func (s *BaseOdpsParserListener) EnterVariableName(ctx *VariableNameContext) {}

// ExitVariableName is called when production variableName is exited.
func (s *BaseOdpsParserListener) ExitVariableName(ctx *VariableNameContext) {}

// EnterAtomExpression is called when production atomExpression is entered.
func (s *BaseOdpsParserListener) EnterAtomExpression(ctx *AtomExpressionContext) {}

// ExitAtomExpression is called when production atomExpression is exited.
func (s *BaseOdpsParserListener) ExitAtomExpression(ctx *AtomExpressionContext) {}

// EnterVariableRef is called when production variableRef is entered.
func (s *BaseOdpsParserListener) EnterVariableRef(ctx *VariableRefContext) {}

// ExitVariableRef is called when production variableRef is exited.
func (s *BaseOdpsParserListener) ExitVariableRef(ctx *VariableRefContext) {}

// EnterVariableCall is called when production variableCall is entered.
func (s *BaseOdpsParserListener) EnterVariableCall(ctx *VariableCallContext) {}

// ExitVariableCall is called when production variableCall is exited.
func (s *BaseOdpsParserListener) ExitVariableCall(ctx *VariableCallContext) {}

// EnterFunNameRef is called when production funNameRef is entered.
func (s *BaseOdpsParserListener) EnterFunNameRef(ctx *FunNameRefContext) {}

// ExitFunNameRef is called when production funNameRef is exited.
func (s *BaseOdpsParserListener) ExitFunNameRef(ctx *FunNameRefContext) {}

// EnterLambdaExpression is called when production lambdaExpression is entered.
func (s *BaseOdpsParserListener) EnterLambdaExpression(ctx *LambdaExpressionContext) {}

// ExitLambdaExpression is called when production lambdaExpression is exited.
func (s *BaseOdpsParserListener) ExitLambdaExpression(ctx *LambdaExpressionContext) {}

// EnterLambdaParameter is called when production lambdaParameter is entered.
func (s *BaseOdpsParserListener) EnterLambdaParameter(ctx *LambdaParameterContext) {}

// ExitLambdaParameter is called when production lambdaParameter is exited.
func (s *BaseOdpsParserListener) ExitLambdaParameter(ctx *LambdaParameterContext) {}

// EnterTableOrColumnRef is called when production tableOrColumnRef is entered.
func (s *BaseOdpsParserListener) EnterTableOrColumnRef(ctx *TableOrColumnRefContext) {}

// ExitTableOrColumnRef is called when production tableOrColumnRef is exited.
func (s *BaseOdpsParserListener) ExitTableOrColumnRef(ctx *TableOrColumnRefContext) {}

// EnterConstructExpression is called when production constructExpression is entered.
func (s *BaseOdpsParserListener) EnterConstructExpression(ctx *ConstructExpressionContext) {}

// ExitConstructExpression is called when production constructExpression is exited.
func (s *BaseOdpsParserListener) ExitConstructExpression(ctx *ConstructExpressionContext) {}

// EnterExistsExpression is called when production existsExpression is entered.
func (s *BaseOdpsParserListener) EnterExistsExpression(ctx *ExistsExpressionContext) {}

// ExitExistsExpression is called when production existsExpression is exited.
func (s *BaseOdpsParserListener) ExitExistsExpression(ctx *ExistsExpressionContext) {}

// EnterScalarSubQueryExpression is called when production scalarSubQueryExpression is entered.
func (s *BaseOdpsParserListener) EnterScalarSubQueryExpression(ctx *ScalarSubQueryExpressionContext) {
}

// ExitScalarSubQueryExpression is called when production scalarSubQueryExpression is exited.
func (s *BaseOdpsParserListener) ExitScalarSubQueryExpression(ctx *ScalarSubQueryExpressionContext) {}

// EnterClassNameWithPackage is called when production classNameWithPackage is entered.
func (s *BaseOdpsParserListener) EnterClassNameWithPackage(ctx *ClassNameWithPackageContext) {}

// ExitClassNameWithPackage is called when production classNameWithPackage is exited.
func (s *BaseOdpsParserListener) ExitClassNameWithPackage(ctx *ClassNameWithPackageContext) {}

// EnterClassNameOrArrayDecl is called when production classNameOrArrayDecl is entered.
func (s *BaseOdpsParserListener) EnterClassNameOrArrayDecl(ctx *ClassNameOrArrayDeclContext) {}

// ExitClassNameOrArrayDecl is called when production classNameOrArrayDecl is exited.
func (s *BaseOdpsParserListener) ExitClassNameOrArrayDecl(ctx *ClassNameOrArrayDeclContext) {}

// EnterClassNameList is called when production classNameList is entered.
func (s *BaseOdpsParserListener) EnterClassNameList(ctx *ClassNameListContext) {}

// ExitClassNameList is called when production classNameList is exited.
func (s *BaseOdpsParserListener) ExitClassNameList(ctx *ClassNameListContext) {}

// EnterOdpsqlNonReserved is called when production odpsqlNonReserved is entered.
func (s *BaseOdpsParserListener) EnterOdpsqlNonReserved(ctx *OdpsqlNonReservedContext) {}

// ExitOdpsqlNonReserved is called when production odpsqlNonReserved is exited.
func (s *BaseOdpsParserListener) ExitOdpsqlNonReserved(ctx *OdpsqlNonReservedContext) {}

// EnterRelaxedKeywords is called when production relaxedKeywords is entered.
func (s *BaseOdpsParserListener) EnterRelaxedKeywords(ctx *RelaxedKeywordsContext) {}

// ExitRelaxedKeywords is called when production relaxedKeywords is exited.
func (s *BaseOdpsParserListener) ExitRelaxedKeywords(ctx *RelaxedKeywordsContext) {}

// EnterAllIdentifiers is called when production allIdentifiers is entered.
func (s *BaseOdpsParserListener) EnterAllIdentifiers(ctx *AllIdentifiersContext) {}

// ExitAllIdentifiers is called when production allIdentifiers is exited.
func (s *BaseOdpsParserListener) ExitAllIdentifiers(ctx *AllIdentifiersContext) {}

// EnterIdentifier is called when production identifier is entered.
func (s *BaseOdpsParserListener) EnterIdentifier(ctx *IdentifierContext) {}

// ExitIdentifier is called when production identifier is exited.
func (s *BaseOdpsParserListener) ExitIdentifier(ctx *IdentifierContext) {}

// EnterAliasIdentifier is called when production aliasIdentifier is entered.
func (s *BaseOdpsParserListener) EnterAliasIdentifier(ctx *AliasIdentifierContext) {}

// ExitAliasIdentifier is called when production aliasIdentifier is exited.
func (s *BaseOdpsParserListener) ExitAliasIdentifier(ctx *AliasIdentifierContext) {}

// EnterIdentifierWithoutSql11 is called when production identifierWithoutSql11 is entered.
func (s *BaseOdpsParserListener) EnterIdentifierWithoutSql11(ctx *IdentifierWithoutSql11Context) {}

// ExitIdentifierWithoutSql11 is called when production identifierWithoutSql11 is exited.
func (s *BaseOdpsParserListener) ExitIdentifierWithoutSql11(ctx *IdentifierWithoutSql11Context) {}

// EnterAlterTableChangeOwner is called when production alterTableChangeOwner is entered.
func (s *BaseOdpsParserListener) EnterAlterTableChangeOwner(ctx *AlterTableChangeOwnerContext) {}

// ExitAlterTableChangeOwner is called when production alterTableChangeOwner is exited.
func (s *BaseOdpsParserListener) ExitAlterTableChangeOwner(ctx *AlterTableChangeOwnerContext) {}

// EnterAlterViewChangeOwner is called when production alterViewChangeOwner is entered.
func (s *BaseOdpsParserListener) EnterAlterViewChangeOwner(ctx *AlterViewChangeOwnerContext) {}

// ExitAlterViewChangeOwner is called when production alterViewChangeOwner is exited.
func (s *BaseOdpsParserListener) ExitAlterViewChangeOwner(ctx *AlterViewChangeOwnerContext) {}

// EnterAlterTableEnableHubTable is called when production alterTableEnableHubTable is entered.
func (s *BaseOdpsParserListener) EnterAlterTableEnableHubTable(ctx *AlterTableEnableHubTableContext) {
}

// ExitAlterTableEnableHubTable is called when production alterTableEnableHubTable is exited.
func (s *BaseOdpsParserListener) ExitAlterTableEnableHubTable(ctx *AlterTableEnableHubTableContext) {}

// EnterTableLifecycle is called when production tableLifecycle is entered.
func (s *BaseOdpsParserListener) EnterTableLifecycle(ctx *TableLifecycleContext) {}

// ExitTableLifecycle is called when production tableLifecycle is exited.
func (s *BaseOdpsParserListener) ExitTableLifecycle(ctx *TableLifecycleContext) {}

// EnterSetStatement is called when production setStatement is entered.
func (s *BaseOdpsParserListener) EnterSetStatement(ctx *SetStatementContext) {}

// ExitSetStatement is called when production setStatement is exited.
func (s *BaseOdpsParserListener) ExitSetStatement(ctx *SetStatementContext) {}

// EnterAnythingButEqualOrSemi is called when production anythingButEqualOrSemi is entered.
func (s *BaseOdpsParserListener) EnterAnythingButEqualOrSemi(ctx *AnythingButEqualOrSemiContext) {}

// ExitAnythingButEqualOrSemi is called when production anythingButEqualOrSemi is exited.
func (s *BaseOdpsParserListener) ExitAnythingButEqualOrSemi(ctx *AnythingButEqualOrSemiContext) {}

// EnterAnythingButSemi is called when production anythingButSemi is entered.
func (s *BaseOdpsParserListener) EnterAnythingButSemi(ctx *AnythingButSemiContext) {}

// ExitAnythingButSemi is called when production anythingButSemi is exited.
func (s *BaseOdpsParserListener) ExitAnythingButSemi(ctx *AnythingButSemiContext) {}

// EnterSetProjectStatement is called when production setProjectStatement is entered.
func (s *BaseOdpsParserListener) EnterSetProjectStatement(ctx *SetProjectStatementContext) {}

// ExitSetProjectStatement is called when production setProjectStatement is exited.
func (s *BaseOdpsParserListener) ExitSetProjectStatement(ctx *SetProjectStatementContext) {}

// EnterLabel is called when production label is entered.
func (s *BaseOdpsParserListener) EnterLabel(ctx *LabelContext) {}

// ExitLabel is called when production label is exited.
func (s *BaseOdpsParserListener) ExitLabel(ctx *LabelContext) {}

// EnterSkewInfoVal is called when production skewInfoVal is entered.
func (s *BaseOdpsParserListener) EnterSkewInfoVal(ctx *SkewInfoValContext) {}

// ExitSkewInfoVal is called when production skewInfoVal is exited.
func (s *BaseOdpsParserListener) ExitSkewInfoVal(ctx *SkewInfoValContext) {}

// EnterMemberAccessOperator is called when production memberAccessOperator is entered.
func (s *BaseOdpsParserListener) EnterMemberAccessOperator(ctx *MemberAccessOperatorContext) {}

// ExitMemberAccessOperator is called when production memberAccessOperator is exited.
func (s *BaseOdpsParserListener) ExitMemberAccessOperator(ctx *MemberAccessOperatorContext) {}

// EnterMethodAccessOperator is called when production methodAccessOperator is entered.
func (s *BaseOdpsParserListener) EnterMethodAccessOperator(ctx *MethodAccessOperatorContext) {}

// ExitMethodAccessOperator is called when production methodAccessOperator is exited.
func (s *BaseOdpsParserListener) ExitMethodAccessOperator(ctx *MethodAccessOperatorContext) {}

// EnterIsNullOperator is called when production isNullOperator is entered.
func (s *BaseOdpsParserListener) EnterIsNullOperator(ctx *IsNullOperatorContext) {}

// ExitIsNullOperator is called when production isNullOperator is exited.
func (s *BaseOdpsParserListener) ExitIsNullOperator(ctx *IsNullOperatorContext) {}

// EnterInOperator is called when production inOperator is entered.
func (s *BaseOdpsParserListener) EnterInOperator(ctx *InOperatorContext) {}

// ExitInOperator is called when production inOperator is exited.
func (s *BaseOdpsParserListener) ExitInOperator(ctx *InOperatorContext) {}

// EnterBetweenOperator is called when production betweenOperator is entered.
func (s *BaseOdpsParserListener) EnterBetweenOperator(ctx *BetweenOperatorContext) {}

// ExitBetweenOperator is called when production betweenOperator is exited.
func (s *BaseOdpsParserListener) ExitBetweenOperator(ctx *BetweenOperatorContext) {}

// EnterMathExpression is called when production mathExpression is entered.
func (s *BaseOdpsParserListener) EnterMathExpression(ctx *MathExpressionContext) {}

// ExitMathExpression is called when production mathExpression is exited.
func (s *BaseOdpsParserListener) ExitMathExpression(ctx *MathExpressionContext) {}

// EnterUnarySuffixExpression is called when production unarySuffixExpression is entered.
func (s *BaseOdpsParserListener) EnterUnarySuffixExpression(ctx *UnarySuffixExpressionContext) {}

// ExitUnarySuffixExpression is called when production unarySuffixExpression is exited.
func (s *BaseOdpsParserListener) ExitUnarySuffixExpression(ctx *UnarySuffixExpressionContext) {}

// EnterUnaryPrefixExpression is called when production unaryPrefixExpression is entered.
func (s *BaseOdpsParserListener) EnterUnaryPrefixExpression(ctx *UnaryPrefixExpressionContext) {}

// ExitUnaryPrefixExpression is called when production unaryPrefixExpression is exited.
func (s *BaseOdpsParserListener) ExitUnaryPrefixExpression(ctx *UnaryPrefixExpressionContext) {}

// EnterFieldExpression is called when production fieldExpression is entered.
func (s *BaseOdpsParserListener) EnterFieldExpression(ctx *FieldExpressionContext) {}

// ExitFieldExpression is called when production fieldExpression is exited.
func (s *BaseOdpsParserListener) ExitFieldExpression(ctx *FieldExpressionContext) {}

// EnterLogicalExpression is called when production logicalExpression is entered.
func (s *BaseOdpsParserListener) EnterLogicalExpression(ctx *LogicalExpressionContext) {}

// ExitLogicalExpression is called when production logicalExpression is exited.
func (s *BaseOdpsParserListener) ExitLogicalExpression(ctx *LogicalExpressionContext) {}

// EnterNotExpression is called when production notExpression is entered.
func (s *BaseOdpsParserListener) EnterNotExpression(ctx *NotExpressionContext) {}

// ExitNotExpression is called when production notExpression is exited.
func (s *BaseOdpsParserListener) ExitNotExpression(ctx *NotExpressionContext) {}

// EnterEqualExpression is called when production equalExpression is entered.
func (s *BaseOdpsParserListener) EnterEqualExpression(ctx *EqualExpressionContext) {}

// ExitEqualExpression is called when production equalExpression is exited.
func (s *BaseOdpsParserListener) ExitEqualExpression(ctx *EqualExpressionContext) {}

// EnterMathExpressionListInParentheses is called when production mathExpressionListInParentheses is entered.
func (s *BaseOdpsParserListener) EnterMathExpressionListInParentheses(ctx *MathExpressionListInParenthesesContext) {
}

// ExitMathExpressionListInParentheses is called when production mathExpressionListInParentheses is exited.
func (s *BaseOdpsParserListener) ExitMathExpressionListInParentheses(ctx *MathExpressionListInParenthesesContext) {
}

// EnterMathExpressionList is called when production mathExpressionList is entered.
func (s *BaseOdpsParserListener) EnterMathExpressionList(ctx *MathExpressionListContext) {}

// ExitMathExpressionList is called when production mathExpressionList is exited.
func (s *BaseOdpsParserListener) ExitMathExpressionList(ctx *MathExpressionListContext) {}

// EnterExpression is called when production expression is entered.
func (s *BaseOdpsParserListener) EnterExpression(ctx *ExpressionContext) {}

// ExitExpression is called when production expression is exited.
func (s *BaseOdpsParserListener) ExitExpression(ctx *ExpressionContext) {}

// EnterStatisticStatement is called when production statisticStatement is entered.
func (s *BaseOdpsParserListener) EnterStatisticStatement(ctx *StatisticStatementContext) {}

// ExitStatisticStatement is called when production statisticStatement is exited.
func (s *BaseOdpsParserListener) ExitStatisticStatement(ctx *StatisticStatementContext) {}

// EnterAddRemoveStatisticStatement is called when production addRemoveStatisticStatement is entered.
func (s *BaseOdpsParserListener) EnterAddRemoveStatisticStatement(ctx *AddRemoveStatisticStatementContext) {
}

// ExitAddRemoveStatisticStatement is called when production addRemoveStatisticStatement is exited.
func (s *BaseOdpsParserListener) ExitAddRemoveStatisticStatement(ctx *AddRemoveStatisticStatementContext) {
}

// EnterStatisticInfo is called when production statisticInfo is entered.
func (s *BaseOdpsParserListener) EnterStatisticInfo(ctx *StatisticInfoContext) {}

// ExitStatisticInfo is called when production statisticInfo is exited.
func (s *BaseOdpsParserListener) ExitStatisticInfo(ctx *StatisticInfoContext) {}

// EnterShowStatisticStatement is called when production showStatisticStatement is entered.
func (s *BaseOdpsParserListener) EnterShowStatisticStatement(ctx *ShowStatisticStatementContext) {}

// ExitShowStatisticStatement is called when production showStatisticStatement is exited.
func (s *BaseOdpsParserListener) ExitShowStatisticStatement(ctx *ShowStatisticStatementContext) {}

// EnterShowStatisticListStatement is called when production showStatisticListStatement is entered.
func (s *BaseOdpsParserListener) EnterShowStatisticListStatement(ctx *ShowStatisticListStatementContext) {
}

// ExitShowStatisticListStatement is called when production showStatisticListStatement is exited.
func (s *BaseOdpsParserListener) ExitShowStatisticListStatement(ctx *ShowStatisticListStatementContext) {
}

// EnterCountTableStatement is called when production countTableStatement is entered.
func (s *BaseOdpsParserListener) EnterCountTableStatement(ctx *CountTableStatementContext) {}

// ExitCountTableStatement is called when production countTableStatement is exited.
func (s *BaseOdpsParserListener) ExitCountTableStatement(ctx *CountTableStatementContext) {}

// EnterStatisticName is called when production statisticName is entered.
func (s *BaseOdpsParserListener) EnterStatisticName(ctx *StatisticNameContext) {}

// ExitStatisticName is called when production statisticName is exited.
func (s *BaseOdpsParserListener) ExitStatisticName(ctx *StatisticNameContext) {}

// EnterInstanceManagement is called when production instanceManagement is entered.
func (s *BaseOdpsParserListener) EnterInstanceManagement(ctx *InstanceManagementContext) {}

// ExitInstanceManagement is called when production instanceManagement is exited.
func (s *BaseOdpsParserListener) ExitInstanceManagement(ctx *InstanceManagementContext) {}

// EnterInstanceStatus is called when production instanceStatus is entered.
func (s *BaseOdpsParserListener) EnterInstanceStatus(ctx *InstanceStatusContext) {}

// ExitInstanceStatus is called when production instanceStatus is exited.
func (s *BaseOdpsParserListener) ExitInstanceStatus(ctx *InstanceStatusContext) {}

// EnterKillInstance is called when production killInstance is entered.
func (s *BaseOdpsParserListener) EnterKillInstance(ctx *KillInstanceContext) {}

// ExitKillInstance is called when production killInstance is exited.
func (s *BaseOdpsParserListener) ExitKillInstance(ctx *KillInstanceContext) {}

// EnterInstanceId is called when production instanceId is entered.
func (s *BaseOdpsParserListener) EnterInstanceId(ctx *InstanceIdContext) {}

// ExitInstanceId is called when production instanceId is exited.
func (s *BaseOdpsParserListener) ExitInstanceId(ctx *InstanceIdContext) {}

// EnterResourceManagement is called when production resourceManagement is entered.
func (s *BaseOdpsParserListener) EnterResourceManagement(ctx *ResourceManagementContext) {}

// ExitResourceManagement is called when production resourceManagement is exited.
func (s *BaseOdpsParserListener) ExitResourceManagement(ctx *ResourceManagementContext) {}

// EnterAddResource is called when production addResource is entered.
func (s *BaseOdpsParserListener) EnterAddResource(ctx *AddResourceContext) {}

// ExitAddResource is called when production addResource is exited.
func (s *BaseOdpsParserListener) ExitAddResource(ctx *AddResourceContext) {}

// EnterDropResource is called when production dropResource is entered.
func (s *BaseOdpsParserListener) EnterDropResource(ctx *DropResourceContext) {}

// ExitDropResource is called when production dropResource is exited.
func (s *BaseOdpsParserListener) ExitDropResource(ctx *DropResourceContext) {}

// EnterResourceId is called when production resourceId is entered.
func (s *BaseOdpsParserListener) EnterResourceId(ctx *ResourceIdContext) {}

// ExitResourceId is called when production resourceId is exited.
func (s *BaseOdpsParserListener) ExitResourceId(ctx *ResourceIdContext) {}

// EnterDropOfflineModel is called when production dropOfflineModel is entered.
func (s *BaseOdpsParserListener) EnterDropOfflineModel(ctx *DropOfflineModelContext) {}

// ExitDropOfflineModel is called when production dropOfflineModel is exited.
func (s *BaseOdpsParserListener) ExitDropOfflineModel(ctx *DropOfflineModelContext) {}

// EnterGetResource is called when production getResource is entered.
func (s *BaseOdpsParserListener) EnterGetResource(ctx *GetResourceContext) {}

// ExitGetResource is called when production getResource is exited.
func (s *BaseOdpsParserListener) ExitGetResource(ctx *GetResourceContext) {}

// EnterOptions is called when production options is entered.
func (s *BaseOdpsParserListener) EnterOptions(ctx *OptionsContext) {}

// ExitOptions is called when production options is exited.
func (s *BaseOdpsParserListener) ExitOptions(ctx *OptionsContext) {}

// EnterAuthorizationStatement is called when production authorizationStatement is entered.
func (s *BaseOdpsParserListener) EnterAuthorizationStatement(ctx *AuthorizationStatementContext) {}

// ExitAuthorizationStatement is called when production authorizationStatement is exited.
func (s *BaseOdpsParserListener) ExitAuthorizationStatement(ctx *AuthorizationStatementContext) {}

// EnterListUsers is called when production listUsers is entered.
func (s *BaseOdpsParserListener) EnterListUsers(ctx *ListUsersContext) {}

// ExitListUsers is called when production listUsers is exited.
func (s *BaseOdpsParserListener) ExitListUsers(ctx *ListUsersContext) {}

// EnterListGroups is called when production listGroups is entered.
func (s *BaseOdpsParserListener) EnterListGroups(ctx *ListGroupsContext) {}

// ExitListGroups is called when production listGroups is exited.
func (s *BaseOdpsParserListener) ExitListGroups(ctx *ListGroupsContext) {}

// EnterAddUserStatement is called when production addUserStatement is entered.
func (s *BaseOdpsParserListener) EnterAddUserStatement(ctx *AddUserStatementContext) {}

// ExitAddUserStatement is called when production addUserStatement is exited.
func (s *BaseOdpsParserListener) ExitAddUserStatement(ctx *AddUserStatementContext) {}

// EnterAddGroupStatement is called when production addGroupStatement is entered.
func (s *BaseOdpsParserListener) EnterAddGroupStatement(ctx *AddGroupStatementContext) {}

// ExitAddGroupStatement is called when production addGroupStatement is exited.
func (s *BaseOdpsParserListener) ExitAddGroupStatement(ctx *AddGroupStatementContext) {}

// EnterRemoveUserStatement is called when production removeUserStatement is entered.
func (s *BaseOdpsParserListener) EnterRemoveUserStatement(ctx *RemoveUserStatementContext) {}

// ExitRemoveUserStatement is called when production removeUserStatement is exited.
func (s *BaseOdpsParserListener) ExitRemoveUserStatement(ctx *RemoveUserStatementContext) {}

// EnterRemoveGroupStatement is called when production removeGroupStatement is entered.
func (s *BaseOdpsParserListener) EnterRemoveGroupStatement(ctx *RemoveGroupStatementContext) {}

// ExitRemoveGroupStatement is called when production removeGroupStatement is exited.
func (s *BaseOdpsParserListener) ExitRemoveGroupStatement(ctx *RemoveGroupStatementContext) {}

// EnterAddAccountProvider is called when production addAccountProvider is entered.
func (s *BaseOdpsParserListener) EnterAddAccountProvider(ctx *AddAccountProviderContext) {}

// ExitAddAccountProvider is called when production addAccountProvider is exited.
func (s *BaseOdpsParserListener) ExitAddAccountProvider(ctx *AddAccountProviderContext) {}

// EnterRemoveAccountProvider is called when production removeAccountProvider is entered.
func (s *BaseOdpsParserListener) EnterRemoveAccountProvider(ctx *RemoveAccountProviderContext) {}

// ExitRemoveAccountProvider is called when production removeAccountProvider is exited.
func (s *BaseOdpsParserListener) ExitRemoveAccountProvider(ctx *RemoveAccountProviderContext) {}

// EnterShowAcl is called when production showAcl is entered.
func (s *BaseOdpsParserListener) EnterShowAcl(ctx *ShowAclContext) {}

// ExitShowAcl is called when production showAcl is exited.
func (s *BaseOdpsParserListener) ExitShowAcl(ctx *ShowAclContext) {}

// EnterListRoles is called when production listRoles is entered.
func (s *BaseOdpsParserListener) EnterListRoles(ctx *ListRolesContext) {}

// ExitListRoles is called when production listRoles is exited.
func (s *BaseOdpsParserListener) ExitListRoles(ctx *ListRolesContext) {}

// EnterWhoami is called when production whoami is entered.
func (s *BaseOdpsParserListener) EnterWhoami(ctx *WhoamiContext) {}

// ExitWhoami is called when production whoami is exited.
func (s *BaseOdpsParserListener) ExitWhoami(ctx *WhoamiContext) {}

// EnterListTrustedProjects is called when production listTrustedProjects is entered.
func (s *BaseOdpsParserListener) EnterListTrustedProjects(ctx *ListTrustedProjectsContext) {}

// ExitListTrustedProjects is called when production listTrustedProjects is exited.
func (s *BaseOdpsParserListener) ExitListTrustedProjects(ctx *ListTrustedProjectsContext) {}

// EnterAddTrustedProject is called when production addTrustedProject is entered.
func (s *BaseOdpsParserListener) EnterAddTrustedProject(ctx *AddTrustedProjectContext) {}

// ExitAddTrustedProject is called when production addTrustedProject is exited.
func (s *BaseOdpsParserListener) ExitAddTrustedProject(ctx *AddTrustedProjectContext) {}

// EnterRemoveTrustedProject is called when production removeTrustedProject is entered.
func (s *BaseOdpsParserListener) EnterRemoveTrustedProject(ctx *RemoveTrustedProjectContext) {}

// ExitRemoveTrustedProject is called when production removeTrustedProject is exited.
func (s *BaseOdpsParserListener) ExitRemoveTrustedProject(ctx *RemoveTrustedProjectContext) {}

// EnterShowSecurityConfiguration is called when production showSecurityConfiguration is entered.
func (s *BaseOdpsParserListener) EnterShowSecurityConfiguration(ctx *ShowSecurityConfigurationContext) {
}

// ExitShowSecurityConfiguration is called when production showSecurityConfiguration is exited.
func (s *BaseOdpsParserListener) ExitShowSecurityConfiguration(ctx *ShowSecurityConfigurationContext) {
}

// EnterShowPackages is called when production showPackages is entered.
func (s *BaseOdpsParserListener) EnterShowPackages(ctx *ShowPackagesContext) {}

// ExitShowPackages is called when production showPackages is exited.
func (s *BaseOdpsParserListener) ExitShowPackages(ctx *ShowPackagesContext) {}

// EnterShowItems is called when production showItems is entered.
func (s *BaseOdpsParserListener) EnterShowItems(ctx *ShowItemsContext) {}

// ExitShowItems is called when production showItems is exited.
func (s *BaseOdpsParserListener) ExitShowItems(ctx *ShowItemsContext) {}

// EnterInstallPackage is called when production installPackage is entered.
func (s *BaseOdpsParserListener) EnterInstallPackage(ctx *InstallPackageContext) {}

// ExitInstallPackage is called when production installPackage is exited.
func (s *BaseOdpsParserListener) ExitInstallPackage(ctx *InstallPackageContext) {}

// EnterUninstallPackage is called when production uninstallPackage is entered.
func (s *BaseOdpsParserListener) EnterUninstallPackage(ctx *UninstallPackageContext) {}

// ExitUninstallPackage is called when production uninstallPackage is exited.
func (s *BaseOdpsParserListener) ExitUninstallPackage(ctx *UninstallPackageContext) {}

// EnterCreatePackage is called when production createPackage is entered.
func (s *BaseOdpsParserListener) EnterCreatePackage(ctx *CreatePackageContext) {}

// ExitCreatePackage is called when production createPackage is exited.
func (s *BaseOdpsParserListener) ExitCreatePackage(ctx *CreatePackageContext) {}

// EnterDeletePackage is called when production deletePackage is entered.
func (s *BaseOdpsParserListener) EnterDeletePackage(ctx *DeletePackageContext) {}

// ExitDeletePackage is called when production deletePackage is exited.
func (s *BaseOdpsParserListener) ExitDeletePackage(ctx *DeletePackageContext) {}

// EnterAddToPackage is called when production addToPackage is entered.
func (s *BaseOdpsParserListener) EnterAddToPackage(ctx *AddToPackageContext) {}

// ExitAddToPackage is called when production addToPackage is exited.
func (s *BaseOdpsParserListener) ExitAddToPackage(ctx *AddToPackageContext) {}

// EnterRemoveFromPackage is called when production removeFromPackage is entered.
func (s *BaseOdpsParserListener) EnterRemoveFromPackage(ctx *RemoveFromPackageContext) {}

// ExitRemoveFromPackage is called when production removeFromPackage is exited.
func (s *BaseOdpsParserListener) ExitRemoveFromPackage(ctx *RemoveFromPackageContext) {}

// EnterAllowPackage is called when production allowPackage is entered.
func (s *BaseOdpsParserListener) EnterAllowPackage(ctx *AllowPackageContext) {}

// ExitAllowPackage is called when production allowPackage is exited.
func (s *BaseOdpsParserListener) ExitAllowPackage(ctx *AllowPackageContext) {}

// EnterDisallowPackage is called when production disallowPackage is entered.
func (s *BaseOdpsParserListener) EnterDisallowPackage(ctx *DisallowPackageContext) {}

// ExitDisallowPackage is called when production disallowPackage is exited.
func (s *BaseOdpsParserListener) ExitDisallowPackage(ctx *DisallowPackageContext) {}

// EnterPutPolicy is called when production putPolicy is entered.
func (s *BaseOdpsParserListener) EnterPutPolicy(ctx *PutPolicyContext) {}

// ExitPutPolicy is called when production putPolicy is exited.
func (s *BaseOdpsParserListener) ExitPutPolicy(ctx *PutPolicyContext) {}

// EnterGetPolicy is called when production getPolicy is entered.
func (s *BaseOdpsParserListener) EnterGetPolicy(ctx *GetPolicyContext) {}

// ExitGetPolicy is called when production getPolicy is exited.
func (s *BaseOdpsParserListener) ExitGetPolicy(ctx *GetPolicyContext) {}

// EnterClearExpiredGrants is called when production clearExpiredGrants is entered.
func (s *BaseOdpsParserListener) EnterClearExpiredGrants(ctx *ClearExpiredGrantsContext) {}

// ExitClearExpiredGrants is called when production clearExpiredGrants is exited.
func (s *BaseOdpsParserListener) ExitClearExpiredGrants(ctx *ClearExpiredGrantsContext) {}

// EnterGrantLabel is called when production grantLabel is entered.
func (s *BaseOdpsParserListener) EnterGrantLabel(ctx *GrantLabelContext) {}

// ExitGrantLabel is called when production grantLabel is exited.
func (s *BaseOdpsParserListener) ExitGrantLabel(ctx *GrantLabelContext) {}

// EnterRevokeLabel is called when production revokeLabel is entered.
func (s *BaseOdpsParserListener) EnterRevokeLabel(ctx *RevokeLabelContext) {}

// ExitRevokeLabel is called when production revokeLabel is exited.
func (s *BaseOdpsParserListener) ExitRevokeLabel(ctx *RevokeLabelContext) {}

// EnterShowLabel is called when production showLabel is entered.
func (s *BaseOdpsParserListener) EnterShowLabel(ctx *ShowLabelContext) {}

// ExitShowLabel is called when production showLabel is exited.
func (s *BaseOdpsParserListener) ExitShowLabel(ctx *ShowLabelContext) {}

// EnterGrantSuperPrivilege is called when production grantSuperPrivilege is entered.
func (s *BaseOdpsParserListener) EnterGrantSuperPrivilege(ctx *GrantSuperPrivilegeContext) {}

// ExitGrantSuperPrivilege is called when production grantSuperPrivilege is exited.
func (s *BaseOdpsParserListener) ExitGrantSuperPrivilege(ctx *GrantSuperPrivilegeContext) {}

// EnterRevokeSuperPrivilege is called when production revokeSuperPrivilege is entered.
func (s *BaseOdpsParserListener) EnterRevokeSuperPrivilege(ctx *RevokeSuperPrivilegeContext) {}

// ExitRevokeSuperPrivilege is called when production revokeSuperPrivilege is exited.
func (s *BaseOdpsParserListener) ExitRevokeSuperPrivilege(ctx *RevokeSuperPrivilegeContext) {}

// EnterCreateRoleStatement is called when production createRoleStatement is entered.
func (s *BaseOdpsParserListener) EnterCreateRoleStatement(ctx *CreateRoleStatementContext) {}

// ExitCreateRoleStatement is called when production createRoleStatement is exited.
func (s *BaseOdpsParserListener) ExitCreateRoleStatement(ctx *CreateRoleStatementContext) {}

// EnterDropRoleStatement is called when production dropRoleStatement is entered.
func (s *BaseOdpsParserListener) EnterDropRoleStatement(ctx *DropRoleStatementContext) {}

// ExitDropRoleStatement is called when production dropRoleStatement is exited.
func (s *BaseOdpsParserListener) ExitDropRoleStatement(ctx *DropRoleStatementContext) {}

// EnterAddRoleToProject is called when production addRoleToProject is entered.
func (s *BaseOdpsParserListener) EnterAddRoleToProject(ctx *AddRoleToProjectContext) {}

// ExitAddRoleToProject is called when production addRoleToProject is exited.
func (s *BaseOdpsParserListener) ExitAddRoleToProject(ctx *AddRoleToProjectContext) {}

// EnterRemoveRoleFromProject is called when production removeRoleFromProject is entered.
func (s *BaseOdpsParserListener) EnterRemoveRoleFromProject(ctx *RemoveRoleFromProjectContext) {}

// ExitRemoveRoleFromProject is called when production removeRoleFromProject is exited.
func (s *BaseOdpsParserListener) ExitRemoveRoleFromProject(ctx *RemoveRoleFromProjectContext) {}

// EnterGrantRole is called when production grantRole is entered.
func (s *BaseOdpsParserListener) EnterGrantRole(ctx *GrantRoleContext) {}

// ExitGrantRole is called when production grantRole is exited.
func (s *BaseOdpsParserListener) ExitGrantRole(ctx *GrantRoleContext) {}

// EnterRevokeRole is called when production revokeRole is entered.
func (s *BaseOdpsParserListener) EnterRevokeRole(ctx *RevokeRoleContext) {}

// ExitRevokeRole is called when production revokeRole is exited.
func (s *BaseOdpsParserListener) ExitRevokeRole(ctx *RevokeRoleContext) {}

// EnterGrantPrivileges is called when production grantPrivileges is entered.
func (s *BaseOdpsParserListener) EnterGrantPrivileges(ctx *GrantPrivilegesContext) {}

// ExitGrantPrivileges is called when production grantPrivileges is exited.
func (s *BaseOdpsParserListener) ExitGrantPrivileges(ctx *GrantPrivilegesContext) {}

// EnterPrivilegeProperties is called when production privilegeProperties is entered.
func (s *BaseOdpsParserListener) EnterPrivilegeProperties(ctx *PrivilegePropertiesContext) {}

// ExitPrivilegeProperties is called when production privilegeProperties is exited.
func (s *BaseOdpsParserListener) ExitPrivilegeProperties(ctx *PrivilegePropertiesContext) {}

// EnterPrivilegePropertieKeys is called when production privilegePropertieKeys is entered.
func (s *BaseOdpsParserListener) EnterPrivilegePropertieKeys(ctx *PrivilegePropertieKeysContext) {}

// ExitPrivilegePropertieKeys is called when production privilegePropertieKeys is exited.
func (s *BaseOdpsParserListener) ExitPrivilegePropertieKeys(ctx *PrivilegePropertieKeysContext) {}

// EnterRevokePrivileges is called when production revokePrivileges is entered.
func (s *BaseOdpsParserListener) EnterRevokePrivileges(ctx *RevokePrivilegesContext) {}

// ExitRevokePrivileges is called when production revokePrivileges is exited.
func (s *BaseOdpsParserListener) ExitRevokePrivileges(ctx *RevokePrivilegesContext) {}

// EnterPurgePrivileges is called when production purgePrivileges is entered.
func (s *BaseOdpsParserListener) EnterPurgePrivileges(ctx *PurgePrivilegesContext) {}

// ExitPurgePrivileges is called when production purgePrivileges is exited.
func (s *BaseOdpsParserListener) ExitPurgePrivileges(ctx *PurgePrivilegesContext) {}

// EnterShowGrants is called when production showGrants is entered.
func (s *BaseOdpsParserListener) EnterShowGrants(ctx *ShowGrantsContext) {}

// ExitShowGrants is called when production showGrants is exited.
func (s *BaseOdpsParserListener) ExitShowGrants(ctx *ShowGrantsContext) {}

// EnterShowRoleGrants is called when production showRoleGrants is entered.
func (s *BaseOdpsParserListener) EnterShowRoleGrants(ctx *ShowRoleGrantsContext) {}

// ExitShowRoleGrants is called when production showRoleGrants is exited.
func (s *BaseOdpsParserListener) ExitShowRoleGrants(ctx *ShowRoleGrantsContext) {}

// EnterShowRoles is called when production showRoles is entered.
func (s *BaseOdpsParserListener) EnterShowRoles(ctx *ShowRolesContext) {}

// ExitShowRoles is called when production showRoles is exited.
func (s *BaseOdpsParserListener) ExitShowRoles(ctx *ShowRolesContext) {}

// EnterShowRolePrincipals is called when production showRolePrincipals is entered.
func (s *BaseOdpsParserListener) EnterShowRolePrincipals(ctx *ShowRolePrincipalsContext) {}

// ExitShowRolePrincipals is called when production showRolePrincipals is exited.
func (s *BaseOdpsParserListener) ExitShowRolePrincipals(ctx *ShowRolePrincipalsContext) {}

// EnterUser is called when production user is entered.
func (s *BaseOdpsParserListener) EnterUser(ctx *UserContext) {}

// ExitUser is called when production user is exited.
func (s *BaseOdpsParserListener) ExitUser(ctx *UserContext) {}

// EnterUserRoleComments is called when production userRoleComments is entered.
func (s *BaseOdpsParserListener) EnterUserRoleComments(ctx *UserRoleCommentsContext) {}

// ExitUserRoleComments is called when production userRoleComments is exited.
func (s *BaseOdpsParserListener) ExitUserRoleComments(ctx *UserRoleCommentsContext) {}

// EnterAccountProvider is called when production accountProvider is entered.
func (s *BaseOdpsParserListener) EnterAccountProvider(ctx *AccountProviderContext) {}

// ExitAccountProvider is called when production accountProvider is exited.
func (s *BaseOdpsParserListener) ExitAccountProvider(ctx *AccountProviderContext) {}

// EnterProjectName is called when production projectName is entered.
func (s *BaseOdpsParserListener) EnterProjectName(ctx *ProjectNameContext) {}

// ExitProjectName is called when production projectName is exited.
func (s *BaseOdpsParserListener) ExitProjectName(ctx *ProjectNameContext) {}

// EnterPrivilegeObjectName is called when production privilegeObjectName is entered.
func (s *BaseOdpsParserListener) EnterPrivilegeObjectName(ctx *PrivilegeObjectNameContext) {}

// ExitPrivilegeObjectName is called when production privilegeObjectName is exited.
func (s *BaseOdpsParserListener) ExitPrivilegeObjectName(ctx *PrivilegeObjectNameContext) {}

// EnterPrivilegeObjectType is called when production privilegeObjectType is entered.
func (s *BaseOdpsParserListener) EnterPrivilegeObjectType(ctx *PrivilegeObjectTypeContext) {}

// ExitPrivilegeObjectType is called when production privilegeObjectType is exited.
func (s *BaseOdpsParserListener) ExitPrivilegeObjectType(ctx *PrivilegeObjectTypeContext) {}

// EnterRoleName is called when production roleName is entered.
func (s *BaseOdpsParserListener) EnterRoleName(ctx *RoleNameContext) {}

// ExitRoleName is called when production roleName is exited.
func (s *BaseOdpsParserListener) ExitRoleName(ctx *RoleNameContext) {}

// EnterPackageName is called when production packageName is entered.
func (s *BaseOdpsParserListener) EnterPackageName(ctx *PackageNameContext) {}

// ExitPackageName is called when production packageName is exited.
func (s *BaseOdpsParserListener) ExitPackageName(ctx *PackageNameContext) {}

// EnterPackageNameWithProject is called when production packageNameWithProject is entered.
func (s *BaseOdpsParserListener) EnterPackageNameWithProject(ctx *PackageNameWithProjectContext) {}

// ExitPackageNameWithProject is called when production packageNameWithProject is exited.
func (s *BaseOdpsParserListener) ExitPackageNameWithProject(ctx *PackageNameWithProjectContext) {}

// EnterPrincipalSpecification is called when production principalSpecification is entered.
func (s *BaseOdpsParserListener) EnterPrincipalSpecification(ctx *PrincipalSpecificationContext) {}

// ExitPrincipalSpecification is called when production principalSpecification is exited.
func (s *BaseOdpsParserListener) ExitPrincipalSpecification(ctx *PrincipalSpecificationContext) {}

// EnterPrincipalName is called when production principalName is entered.
func (s *BaseOdpsParserListener) EnterPrincipalName(ctx *PrincipalNameContext) {}

// ExitPrincipalName is called when production principalName is exited.
func (s *BaseOdpsParserListener) ExitPrincipalName(ctx *PrincipalNameContext) {}

// EnterPrincipalIdentifier is called when production principalIdentifier is entered.
func (s *BaseOdpsParserListener) EnterPrincipalIdentifier(ctx *PrincipalIdentifierContext) {}

// ExitPrincipalIdentifier is called when production principalIdentifier is exited.
func (s *BaseOdpsParserListener) ExitPrincipalIdentifier(ctx *PrincipalIdentifierContext) {}

// EnterPrivilege is called when production privilege is entered.
func (s *BaseOdpsParserListener) EnterPrivilege(ctx *PrivilegeContext) {}

// ExitPrivilege is called when production privilege is exited.
func (s *BaseOdpsParserListener) ExitPrivilege(ctx *PrivilegeContext) {}

// EnterPrivilegeType is called when production privilegeType is entered.
func (s *BaseOdpsParserListener) EnterPrivilegeType(ctx *PrivilegeTypeContext) {}

// ExitPrivilegeType is called when production privilegeType is exited.
func (s *BaseOdpsParserListener) ExitPrivilegeType(ctx *PrivilegeTypeContext) {}

// EnterPrivilegeObject is called when production privilegeObject is entered.
func (s *BaseOdpsParserListener) EnterPrivilegeObject(ctx *PrivilegeObjectContext) {}

// ExitPrivilegeObject is called when production privilegeObject is exited.
func (s *BaseOdpsParserListener) ExitPrivilegeObject(ctx *PrivilegeObjectContext) {}

// EnterFilePath is called when production filePath is entered.
func (s *BaseOdpsParserListener) EnterFilePath(ctx *FilePathContext) {}

// ExitFilePath is called when production filePath is exited.
func (s *BaseOdpsParserListener) ExitFilePath(ctx *FilePathContext) {}

// EnterPolicyCondition is called when production policyCondition is entered.
func (s *BaseOdpsParserListener) EnterPolicyCondition(ctx *PolicyConditionContext) {}

// ExitPolicyCondition is called when production policyCondition is exited.
func (s *BaseOdpsParserListener) ExitPolicyCondition(ctx *PolicyConditionContext) {}

// EnterPolicyConditionOp is called when production policyConditionOp is entered.
func (s *BaseOdpsParserListener) EnterPolicyConditionOp(ctx *PolicyConditionOpContext) {}

// ExitPolicyConditionOp is called when production policyConditionOp is exited.
func (s *BaseOdpsParserListener) ExitPolicyConditionOp(ctx *PolicyConditionOpContext) {}

// EnterPolicyKey is called when production policyKey is entered.
func (s *BaseOdpsParserListener) EnterPolicyKey(ctx *PolicyKeyContext) {}

// ExitPolicyKey is called when production policyKey is exited.
func (s *BaseOdpsParserListener) ExitPolicyKey(ctx *PolicyKeyContext) {}

// EnterPolicyValue is called when production policyValue is entered.
func (s *BaseOdpsParserListener) EnterPolicyValue(ctx *PolicyValueContext) {}

// ExitPolicyValue is called when production policyValue is exited.
func (s *BaseOdpsParserListener) ExitPolicyValue(ctx *PolicyValueContext) {}

// EnterShowCurrentRole is called when production showCurrentRole is entered.
func (s *BaseOdpsParserListener) EnterShowCurrentRole(ctx *ShowCurrentRoleContext) {}

// ExitShowCurrentRole is called when production showCurrentRole is exited.
func (s *BaseOdpsParserListener) ExitShowCurrentRole(ctx *ShowCurrentRoleContext) {}

// EnterSetRole is called when production setRole is entered.
func (s *BaseOdpsParserListener) EnterSetRole(ctx *SetRoleContext) {}

// ExitSetRole is called when production setRole is exited.
func (s *BaseOdpsParserListener) ExitSetRole(ctx *SetRoleContext) {}

// EnterAdminOptionFor is called when production adminOptionFor is entered.
func (s *BaseOdpsParserListener) EnterAdminOptionFor(ctx *AdminOptionForContext) {}

// ExitAdminOptionFor is called when production adminOptionFor is exited.
func (s *BaseOdpsParserListener) ExitAdminOptionFor(ctx *AdminOptionForContext) {}

// EnterWithAdminOption is called when production withAdminOption is entered.
func (s *BaseOdpsParserListener) EnterWithAdminOption(ctx *WithAdminOptionContext) {}

// ExitWithAdminOption is called when production withAdminOption is exited.
func (s *BaseOdpsParserListener) ExitWithAdminOption(ctx *WithAdminOptionContext) {}

// EnterWithGrantOption is called when production withGrantOption is entered.
func (s *BaseOdpsParserListener) EnterWithGrantOption(ctx *WithGrantOptionContext) {}

// ExitWithGrantOption is called when production withGrantOption is exited.
func (s *BaseOdpsParserListener) ExitWithGrantOption(ctx *WithGrantOptionContext) {}

// EnterGrantOptionFor is called when production grantOptionFor is entered.
func (s *BaseOdpsParserListener) EnterGrantOptionFor(ctx *GrantOptionForContext) {}

// ExitGrantOptionFor is called when production grantOptionFor is exited.
func (s *BaseOdpsParserListener) ExitGrantOptionFor(ctx *GrantOptionForContext) {}

// EnterExplainOption is called when production explainOption is entered.
func (s *BaseOdpsParserListener) EnterExplainOption(ctx *ExplainOptionContext) {}

// ExitExplainOption is called when production explainOption is exited.
func (s *BaseOdpsParserListener) ExitExplainOption(ctx *ExplainOptionContext) {}

// EnterLoadStatement is called when production loadStatement is entered.
func (s *BaseOdpsParserListener) EnterLoadStatement(ctx *LoadStatementContext) {}

// ExitLoadStatement is called when production loadStatement is exited.
func (s *BaseOdpsParserListener) ExitLoadStatement(ctx *LoadStatementContext) {}

// EnterReplicationClause is called when production replicationClause is entered.
func (s *BaseOdpsParserListener) EnterReplicationClause(ctx *ReplicationClauseContext) {}

// ExitReplicationClause is called when production replicationClause is exited.
func (s *BaseOdpsParserListener) ExitReplicationClause(ctx *ReplicationClauseContext) {}

// EnterExportStatement is called when production exportStatement is entered.
func (s *BaseOdpsParserListener) EnterExportStatement(ctx *ExportStatementContext) {}

// ExitExportStatement is called when production exportStatement is exited.
func (s *BaseOdpsParserListener) ExitExportStatement(ctx *ExportStatementContext) {}

// EnterImportStatement is called when production importStatement is entered.
func (s *BaseOdpsParserListener) EnterImportStatement(ctx *ImportStatementContext) {}

// ExitImportStatement is called when production importStatement is exited.
func (s *BaseOdpsParserListener) ExitImportStatement(ctx *ImportStatementContext) {}

// EnterReadStatement is called when production readStatement is entered.
func (s *BaseOdpsParserListener) EnterReadStatement(ctx *ReadStatementContext) {}

// ExitReadStatement is called when production readStatement is exited.
func (s *BaseOdpsParserListener) ExitReadStatement(ctx *ReadStatementContext) {}

// EnterUndoStatement is called when production undoStatement is entered.
func (s *BaseOdpsParserListener) EnterUndoStatement(ctx *UndoStatementContext) {}

// ExitUndoStatement is called when production undoStatement is exited.
func (s *BaseOdpsParserListener) ExitUndoStatement(ctx *UndoStatementContext) {}

// EnterRedoStatement is called when production redoStatement is entered.
func (s *BaseOdpsParserListener) EnterRedoStatement(ctx *RedoStatementContext) {}

// ExitRedoStatement is called when production redoStatement is exited.
func (s *BaseOdpsParserListener) ExitRedoStatement(ctx *RedoStatementContext) {}

// EnterPurgeStatement is called when production purgeStatement is entered.
func (s *BaseOdpsParserListener) EnterPurgeStatement(ctx *PurgeStatementContext) {}

// ExitPurgeStatement is called when production purgeStatement is exited.
func (s *BaseOdpsParserListener) ExitPurgeStatement(ctx *PurgeStatementContext) {}

// EnterDropTableVairableStatement is called when production dropTableVairableStatement is entered.
func (s *BaseOdpsParserListener) EnterDropTableVairableStatement(ctx *DropTableVairableStatementContext) {
}

// ExitDropTableVairableStatement is called when production dropTableVairableStatement is exited.
func (s *BaseOdpsParserListener) ExitDropTableVairableStatement(ctx *DropTableVairableStatementContext) {
}

// EnterMsckRepairTableStatement is called when production msckRepairTableStatement is entered.
func (s *BaseOdpsParserListener) EnterMsckRepairTableStatement(ctx *MsckRepairTableStatementContext) {
}

// ExitMsckRepairTableStatement is called when production msckRepairTableStatement is exited.
func (s *BaseOdpsParserListener) ExitMsckRepairTableStatement(ctx *MsckRepairTableStatementContext) {}

// EnterDdlStatement is called when production ddlStatement is entered.
func (s *BaseOdpsParserListener) EnterDdlStatement(ctx *DdlStatementContext) {}

// ExitDdlStatement is called when production ddlStatement is exited.
func (s *BaseOdpsParserListener) ExitDdlStatement(ctx *DdlStatementContext) {}

// EnterPartitionSpecOrPartitionId is called when production partitionSpecOrPartitionId is entered.
func (s *BaseOdpsParserListener) EnterPartitionSpecOrPartitionId(ctx *PartitionSpecOrPartitionIdContext) {
}

// ExitPartitionSpecOrPartitionId is called when production partitionSpecOrPartitionId is exited.
func (s *BaseOdpsParserListener) ExitPartitionSpecOrPartitionId(ctx *PartitionSpecOrPartitionIdContext) {
}

// EnterTableOrTableId is called when production tableOrTableId is entered.
func (s *BaseOdpsParserListener) EnterTableOrTableId(ctx *TableOrTableIdContext) {}

// ExitTableOrTableId is called when production tableOrTableId is exited.
func (s *BaseOdpsParserListener) ExitTableOrTableId(ctx *TableOrTableIdContext) {}

// EnterTableHistoryStatement is called when production tableHistoryStatement is entered.
func (s *BaseOdpsParserListener) EnterTableHistoryStatement(ctx *TableHistoryStatementContext) {}

// ExitTableHistoryStatement is called when production tableHistoryStatement is exited.
func (s *BaseOdpsParserListener) ExitTableHistoryStatement(ctx *TableHistoryStatementContext) {}

// EnterSetExstore is called when production setExstore is entered.
func (s *BaseOdpsParserListener) EnterSetExstore(ctx *SetExstoreContext) {}

// ExitSetExstore is called when production setExstore is exited.
func (s *BaseOdpsParserListener) ExitSetExstore(ctx *SetExstoreContext) {}

// EnterIfExists is called when production ifExists is entered.
func (s *BaseOdpsParserListener) EnterIfExists(ctx *IfExistsContext) {}

// ExitIfExists is called when production ifExists is exited.
func (s *BaseOdpsParserListener) ExitIfExists(ctx *IfExistsContext) {}

// EnterRestrictOrCascade is called when production restrictOrCascade is entered.
func (s *BaseOdpsParserListener) EnterRestrictOrCascade(ctx *RestrictOrCascadeContext) {}

// ExitRestrictOrCascade is called when production restrictOrCascade is exited.
func (s *BaseOdpsParserListener) ExitRestrictOrCascade(ctx *RestrictOrCascadeContext) {}

// EnterIfNotExists is called when production ifNotExists is entered.
func (s *BaseOdpsParserListener) EnterIfNotExists(ctx *IfNotExistsContext) {}

// ExitIfNotExists is called when production ifNotExists is exited.
func (s *BaseOdpsParserListener) ExitIfNotExists(ctx *IfNotExistsContext) {}

// EnterRewriteEnabled is called when production rewriteEnabled is entered.
func (s *BaseOdpsParserListener) EnterRewriteEnabled(ctx *RewriteEnabledContext) {}

// ExitRewriteEnabled is called when production rewriteEnabled is exited.
func (s *BaseOdpsParserListener) ExitRewriteEnabled(ctx *RewriteEnabledContext) {}

// EnterRewriteDisabled is called when production rewriteDisabled is entered.
func (s *BaseOdpsParserListener) EnterRewriteDisabled(ctx *RewriteDisabledContext) {}

// ExitRewriteDisabled is called when production rewriteDisabled is exited.
func (s *BaseOdpsParserListener) ExitRewriteDisabled(ctx *RewriteDisabledContext) {}

// EnterStoredAsDirs is called when production storedAsDirs is entered.
func (s *BaseOdpsParserListener) EnterStoredAsDirs(ctx *StoredAsDirsContext) {}

// ExitStoredAsDirs is called when production storedAsDirs is exited.
func (s *BaseOdpsParserListener) ExitStoredAsDirs(ctx *StoredAsDirsContext) {}

// EnterOrReplace is called when production orReplace is entered.
func (s *BaseOdpsParserListener) EnterOrReplace(ctx *OrReplaceContext) {}

// ExitOrReplace is called when production orReplace is exited.
func (s *BaseOdpsParserListener) ExitOrReplace(ctx *OrReplaceContext) {}

// EnterIgnoreProtection is called when production ignoreProtection is entered.
func (s *BaseOdpsParserListener) EnterIgnoreProtection(ctx *IgnoreProtectionContext) {}

// ExitIgnoreProtection is called when production ignoreProtection is exited.
func (s *BaseOdpsParserListener) ExitIgnoreProtection(ctx *IgnoreProtectionContext) {}

// EnterCreateDatabaseStatement is called when production createDatabaseStatement is entered.
func (s *BaseOdpsParserListener) EnterCreateDatabaseStatement(ctx *CreateDatabaseStatementContext) {}

// ExitCreateDatabaseStatement is called when production createDatabaseStatement is exited.
func (s *BaseOdpsParserListener) ExitCreateDatabaseStatement(ctx *CreateDatabaseStatementContext) {}

// EnterSchemaName is called when production schemaName is entered.
func (s *BaseOdpsParserListener) EnterSchemaName(ctx *SchemaNameContext) {}

// ExitSchemaName is called when production schemaName is exited.
func (s *BaseOdpsParserListener) ExitSchemaName(ctx *SchemaNameContext) {}

// EnterCreateSchemaStatement is called when production createSchemaStatement is entered.
func (s *BaseOdpsParserListener) EnterCreateSchemaStatement(ctx *CreateSchemaStatementContext) {}

// ExitCreateSchemaStatement is called when production createSchemaStatement is exited.
func (s *BaseOdpsParserListener) ExitCreateSchemaStatement(ctx *CreateSchemaStatementContext) {}

// EnterDbLocation is called when production dbLocation is entered.
func (s *BaseOdpsParserListener) EnterDbLocation(ctx *DbLocationContext) {}

// ExitDbLocation is called when production dbLocation is exited.
func (s *BaseOdpsParserListener) ExitDbLocation(ctx *DbLocationContext) {}

// EnterDbProperties is called when production dbProperties is entered.
func (s *BaseOdpsParserListener) EnterDbProperties(ctx *DbPropertiesContext) {}

// ExitDbProperties is called when production dbProperties is exited.
func (s *BaseOdpsParserListener) ExitDbProperties(ctx *DbPropertiesContext) {}

// EnterDbPropertiesList is called when production dbPropertiesList is entered.
func (s *BaseOdpsParserListener) EnterDbPropertiesList(ctx *DbPropertiesListContext) {}

// ExitDbPropertiesList is called when production dbPropertiesList is exited.
func (s *BaseOdpsParserListener) ExitDbPropertiesList(ctx *DbPropertiesListContext) {}

// EnterSwitchDatabaseStatement is called when production switchDatabaseStatement is entered.
func (s *BaseOdpsParserListener) EnterSwitchDatabaseStatement(ctx *SwitchDatabaseStatementContext) {}

// ExitSwitchDatabaseStatement is called when production switchDatabaseStatement is exited.
func (s *BaseOdpsParserListener) ExitSwitchDatabaseStatement(ctx *SwitchDatabaseStatementContext) {}

// EnterDropDatabaseStatement is called when production dropDatabaseStatement is entered.
func (s *BaseOdpsParserListener) EnterDropDatabaseStatement(ctx *DropDatabaseStatementContext) {}

// ExitDropDatabaseStatement is called when production dropDatabaseStatement is exited.
func (s *BaseOdpsParserListener) ExitDropDatabaseStatement(ctx *DropDatabaseStatementContext) {}

// EnterDropSchemaStatement is called when production dropSchemaStatement is entered.
func (s *BaseOdpsParserListener) EnterDropSchemaStatement(ctx *DropSchemaStatementContext) {}

// ExitDropSchemaStatement is called when production dropSchemaStatement is exited.
func (s *BaseOdpsParserListener) ExitDropSchemaStatement(ctx *DropSchemaStatementContext) {}

// EnterDatabaseComment is called when production databaseComment is entered.
func (s *BaseOdpsParserListener) EnterDatabaseComment(ctx *DatabaseCommentContext) {}

// ExitDatabaseComment is called when production databaseComment is exited.
func (s *BaseOdpsParserListener) ExitDatabaseComment(ctx *DatabaseCommentContext) {}

// EnterDataFormatDesc is called when production dataFormatDesc is entered.
func (s *BaseOdpsParserListener) EnterDataFormatDesc(ctx *DataFormatDescContext) {}

// ExitDataFormatDesc is called when production dataFormatDesc is exited.
func (s *BaseOdpsParserListener) ExitDataFormatDesc(ctx *DataFormatDescContext) {}

// EnterCreateTableStatement is called when production createTableStatement is entered.
func (s *BaseOdpsParserListener) EnterCreateTableStatement(ctx *CreateTableStatementContext) {}

// ExitCreateTableStatement is called when production createTableStatement is exited.
func (s *BaseOdpsParserListener) ExitCreateTableStatement(ctx *CreateTableStatementContext) {}

// EnterTruncateTableStatement is called when production truncateTableStatement is entered.
func (s *BaseOdpsParserListener) EnterTruncateTableStatement(ctx *TruncateTableStatementContext) {}

// ExitTruncateTableStatement is called when production truncateTableStatement is exited.
func (s *BaseOdpsParserListener) ExitTruncateTableStatement(ctx *TruncateTableStatementContext) {}

// EnterCreateIndexStatement is called when production createIndexStatement is entered.
func (s *BaseOdpsParserListener) EnterCreateIndexStatement(ctx *CreateIndexStatementContext) {}

// ExitCreateIndexStatement is called when production createIndexStatement is exited.
func (s *BaseOdpsParserListener) ExitCreateIndexStatement(ctx *CreateIndexStatementContext) {}

// EnterIndexComment is called when production indexComment is entered.
func (s *BaseOdpsParserListener) EnterIndexComment(ctx *IndexCommentContext) {}

// ExitIndexComment is called when production indexComment is exited.
func (s *BaseOdpsParserListener) ExitIndexComment(ctx *IndexCommentContext) {}

// EnterAutoRebuild is called when production autoRebuild is entered.
func (s *BaseOdpsParserListener) EnterAutoRebuild(ctx *AutoRebuildContext) {}

// ExitAutoRebuild is called when production autoRebuild is exited.
func (s *BaseOdpsParserListener) ExitAutoRebuild(ctx *AutoRebuildContext) {}

// EnterIndexTblName is called when production indexTblName is entered.
func (s *BaseOdpsParserListener) EnterIndexTblName(ctx *IndexTblNameContext) {}

// ExitIndexTblName is called when production indexTblName is exited.
func (s *BaseOdpsParserListener) ExitIndexTblName(ctx *IndexTblNameContext) {}

// EnterIndexPropertiesPrefixed is called when production indexPropertiesPrefixed is entered.
func (s *BaseOdpsParserListener) EnterIndexPropertiesPrefixed(ctx *IndexPropertiesPrefixedContext) {}

// ExitIndexPropertiesPrefixed is called when production indexPropertiesPrefixed is exited.
func (s *BaseOdpsParserListener) ExitIndexPropertiesPrefixed(ctx *IndexPropertiesPrefixedContext) {}

// EnterIndexProperties is called when production indexProperties is entered.
func (s *BaseOdpsParserListener) EnterIndexProperties(ctx *IndexPropertiesContext) {}

// ExitIndexProperties is called when production indexProperties is exited.
func (s *BaseOdpsParserListener) ExitIndexProperties(ctx *IndexPropertiesContext) {}

// EnterIndexPropertiesList is called when production indexPropertiesList is entered.
func (s *BaseOdpsParserListener) EnterIndexPropertiesList(ctx *IndexPropertiesListContext) {}

// ExitIndexPropertiesList is called when production indexPropertiesList is exited.
func (s *BaseOdpsParserListener) ExitIndexPropertiesList(ctx *IndexPropertiesListContext) {}

// EnterDropIndexStatement is called when production dropIndexStatement is entered.
func (s *BaseOdpsParserListener) EnterDropIndexStatement(ctx *DropIndexStatementContext) {}

// ExitDropIndexStatement is called when production dropIndexStatement is exited.
func (s *BaseOdpsParserListener) ExitDropIndexStatement(ctx *DropIndexStatementContext) {}

// EnterDropTableStatement is called when production dropTableStatement is entered.
func (s *BaseOdpsParserListener) EnterDropTableStatement(ctx *DropTableStatementContext) {}

// ExitDropTableStatement is called when production dropTableStatement is exited.
func (s *BaseOdpsParserListener) ExitDropTableStatement(ctx *DropTableStatementContext) {}

// EnterAlterStatement is called when production alterStatement is entered.
func (s *BaseOdpsParserListener) EnterAlterStatement(ctx *AlterStatementContext) {}

// ExitAlterStatement is called when production alterStatement is exited.
func (s *BaseOdpsParserListener) ExitAlterStatement(ctx *AlterStatementContext) {}

// EnterAlterSchemaStatementSuffix is called when production alterSchemaStatementSuffix is entered.
func (s *BaseOdpsParserListener) EnterAlterSchemaStatementSuffix(ctx *AlterSchemaStatementSuffixContext) {
}

// ExitAlterSchemaStatementSuffix is called when production alterSchemaStatementSuffix is exited.
func (s *BaseOdpsParserListener) ExitAlterSchemaStatementSuffix(ctx *AlterSchemaStatementSuffixContext) {
}

// EnterAlterTableStatementSuffix is called when production alterTableStatementSuffix is entered.
func (s *BaseOdpsParserListener) EnterAlterTableStatementSuffix(ctx *AlterTableStatementSuffixContext) {
}

// ExitAlterTableStatementSuffix is called when production alterTableStatementSuffix is exited.
func (s *BaseOdpsParserListener) ExitAlterTableStatementSuffix(ctx *AlterTableStatementSuffixContext) {
}

// EnterAlterTableMergePartitionSuffix is called when production alterTableMergePartitionSuffix is entered.
func (s *BaseOdpsParserListener) EnterAlterTableMergePartitionSuffix(ctx *AlterTableMergePartitionSuffixContext) {
}

// ExitAlterTableMergePartitionSuffix is called when production alterTableMergePartitionSuffix is exited.
func (s *BaseOdpsParserListener) ExitAlterTableMergePartitionSuffix(ctx *AlterTableMergePartitionSuffixContext) {
}

// EnterAlterStatementSuffixAddConstraint is called when production alterStatementSuffixAddConstraint is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixAddConstraint(ctx *AlterStatementSuffixAddConstraintContext) {
}

// ExitAlterStatementSuffixAddConstraint is called when production alterStatementSuffixAddConstraint is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixAddConstraint(ctx *AlterStatementSuffixAddConstraintContext) {
}

// EnterAlterTblPartitionStatementSuffix is called when production alterTblPartitionStatementSuffix is entered.
func (s *BaseOdpsParserListener) EnterAlterTblPartitionStatementSuffix(ctx *AlterTblPartitionStatementSuffixContext) {
}

// ExitAlterTblPartitionStatementSuffix is called when production alterTblPartitionStatementSuffix is exited.
func (s *BaseOdpsParserListener) ExitAlterTblPartitionStatementSuffix(ctx *AlterTblPartitionStatementSuffixContext) {
}

// EnterAlterStatementSuffixPartitionLifecycle is called when production alterStatementSuffixPartitionLifecycle is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixPartitionLifecycle(ctx *AlterStatementSuffixPartitionLifecycleContext) {
}

// ExitAlterStatementSuffixPartitionLifecycle is called when production alterStatementSuffixPartitionLifecycle is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixPartitionLifecycle(ctx *AlterStatementSuffixPartitionLifecycleContext) {
}

// EnterAlterTblPartitionStatementSuffixProperties is called when production alterTblPartitionStatementSuffixProperties is entered.
func (s *BaseOdpsParserListener) EnterAlterTblPartitionStatementSuffixProperties(ctx *AlterTblPartitionStatementSuffixPropertiesContext) {
}

// ExitAlterTblPartitionStatementSuffixProperties is called when production alterTblPartitionStatementSuffixProperties is exited.
func (s *BaseOdpsParserListener) ExitAlterTblPartitionStatementSuffixProperties(ctx *AlterTblPartitionStatementSuffixPropertiesContext) {
}

// EnterAlterStatementPartitionKeyType is called when production alterStatementPartitionKeyType is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementPartitionKeyType(ctx *AlterStatementPartitionKeyTypeContext) {
}

// ExitAlterStatementPartitionKeyType is called when production alterStatementPartitionKeyType is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementPartitionKeyType(ctx *AlterStatementPartitionKeyTypeContext) {
}

// EnterAlterViewStatementSuffix is called when production alterViewStatementSuffix is entered.
func (s *BaseOdpsParserListener) EnterAlterViewStatementSuffix(ctx *AlterViewStatementSuffixContext) {
}

// ExitAlterViewStatementSuffix is called when production alterViewStatementSuffix is exited.
func (s *BaseOdpsParserListener) ExitAlterViewStatementSuffix(ctx *AlterViewStatementSuffixContext) {}

// EnterAlterMaterializedViewStatementSuffix is called when production alterMaterializedViewStatementSuffix is entered.
func (s *BaseOdpsParserListener) EnterAlterMaterializedViewStatementSuffix(ctx *AlterMaterializedViewStatementSuffixContext) {
}

// ExitAlterMaterializedViewStatementSuffix is called when production alterMaterializedViewStatementSuffix is exited.
func (s *BaseOdpsParserListener) ExitAlterMaterializedViewStatementSuffix(ctx *AlterMaterializedViewStatementSuffixContext) {
}

// EnterAlterMaterializedViewSuffixRewrite is called when production alterMaterializedViewSuffixRewrite is entered.
func (s *BaseOdpsParserListener) EnterAlterMaterializedViewSuffixRewrite(ctx *AlterMaterializedViewSuffixRewriteContext) {
}

// ExitAlterMaterializedViewSuffixRewrite is called when production alterMaterializedViewSuffixRewrite is exited.
func (s *BaseOdpsParserListener) ExitAlterMaterializedViewSuffixRewrite(ctx *AlterMaterializedViewSuffixRewriteContext) {
}

// EnterAlterMaterializedViewSuffixRebuild is called when production alterMaterializedViewSuffixRebuild is entered.
func (s *BaseOdpsParserListener) EnterAlterMaterializedViewSuffixRebuild(ctx *AlterMaterializedViewSuffixRebuildContext) {
}

// ExitAlterMaterializedViewSuffixRebuild is called when production alterMaterializedViewSuffixRebuild is exited.
func (s *BaseOdpsParserListener) ExitAlterMaterializedViewSuffixRebuild(ctx *AlterMaterializedViewSuffixRebuildContext) {
}

// EnterAlterIndexStatementSuffix is called when production alterIndexStatementSuffix is entered.
func (s *BaseOdpsParserListener) EnterAlterIndexStatementSuffix(ctx *AlterIndexStatementSuffixContext) {
}

// ExitAlterIndexStatementSuffix is called when production alterIndexStatementSuffix is exited.
func (s *BaseOdpsParserListener) ExitAlterIndexStatementSuffix(ctx *AlterIndexStatementSuffixContext) {
}

// EnterAlterDatabaseStatementSuffix is called when production alterDatabaseStatementSuffix is entered.
func (s *BaseOdpsParserListener) EnterAlterDatabaseStatementSuffix(ctx *AlterDatabaseStatementSuffixContext) {
}

// ExitAlterDatabaseStatementSuffix is called when production alterDatabaseStatementSuffix is exited.
func (s *BaseOdpsParserListener) ExitAlterDatabaseStatementSuffix(ctx *AlterDatabaseStatementSuffixContext) {
}

// EnterAlterDatabaseSuffixProperties is called when production alterDatabaseSuffixProperties is entered.
func (s *BaseOdpsParserListener) EnterAlterDatabaseSuffixProperties(ctx *AlterDatabaseSuffixPropertiesContext) {
}

// ExitAlterDatabaseSuffixProperties is called when production alterDatabaseSuffixProperties is exited.
func (s *BaseOdpsParserListener) ExitAlterDatabaseSuffixProperties(ctx *AlterDatabaseSuffixPropertiesContext) {
}

// EnterAlterDatabaseSuffixSetOwner is called when production alterDatabaseSuffixSetOwner is entered.
func (s *BaseOdpsParserListener) EnterAlterDatabaseSuffixSetOwner(ctx *AlterDatabaseSuffixSetOwnerContext) {
}

// ExitAlterDatabaseSuffixSetOwner is called when production alterDatabaseSuffixSetOwner is exited.
func (s *BaseOdpsParserListener) ExitAlterDatabaseSuffixSetOwner(ctx *AlterDatabaseSuffixSetOwnerContext) {
}

// EnterAlterStatementSuffixRename is called when production alterStatementSuffixRename is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixRename(ctx *AlterStatementSuffixRenameContext) {
}

// ExitAlterStatementSuffixRename is called when production alterStatementSuffixRename is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixRename(ctx *AlterStatementSuffixRenameContext) {
}

// EnterAlterStatementSuffixAddCol is called when production alterStatementSuffixAddCol is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixAddCol(ctx *AlterStatementSuffixAddColContext) {
}

// ExitAlterStatementSuffixAddCol is called when production alterStatementSuffixAddCol is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixAddCol(ctx *AlterStatementSuffixAddColContext) {
}

// EnterAlterStatementSuffixRenameCol is called when production alterStatementSuffixRenameCol is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixRenameCol(ctx *AlterStatementSuffixRenameColContext) {
}

// ExitAlterStatementSuffixRenameCol is called when production alterStatementSuffixRenameCol is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixRenameCol(ctx *AlterStatementSuffixRenameColContext) {
}

// EnterAlterStatementSuffixDropCol is called when production alterStatementSuffixDropCol is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixDropCol(ctx *AlterStatementSuffixDropColContext) {
}

// ExitAlterStatementSuffixDropCol is called when production alterStatementSuffixDropCol is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixDropCol(ctx *AlterStatementSuffixDropColContext) {
}

// EnterAlterStatementSuffixUpdateStatsCol is called when production alterStatementSuffixUpdateStatsCol is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixUpdateStatsCol(ctx *AlterStatementSuffixUpdateStatsColContext) {
}

// ExitAlterStatementSuffixUpdateStatsCol is called when production alterStatementSuffixUpdateStatsCol is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixUpdateStatsCol(ctx *AlterStatementSuffixUpdateStatsColContext) {
}

// EnterAlterStatementChangeColPosition is called when production alterStatementChangeColPosition is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementChangeColPosition(ctx *AlterStatementChangeColPositionContext) {
}

// ExitAlterStatementChangeColPosition is called when production alterStatementChangeColPosition is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementChangeColPosition(ctx *AlterStatementChangeColPositionContext) {
}

// EnterAlterStatementSuffixAddPartitions is called when production alterStatementSuffixAddPartitions is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixAddPartitions(ctx *AlterStatementSuffixAddPartitionsContext) {
}

// ExitAlterStatementSuffixAddPartitions is called when production alterStatementSuffixAddPartitions is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixAddPartitions(ctx *AlterStatementSuffixAddPartitionsContext) {
}

// EnterAlterStatementSuffixAddPartitionsElement is called when production alterStatementSuffixAddPartitionsElement is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixAddPartitionsElement(ctx *AlterStatementSuffixAddPartitionsElementContext) {
}

// ExitAlterStatementSuffixAddPartitionsElement is called when production alterStatementSuffixAddPartitionsElement is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixAddPartitionsElement(ctx *AlterStatementSuffixAddPartitionsElementContext) {
}

// EnterAlterStatementSuffixTouch is called when production alterStatementSuffixTouch is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixTouch(ctx *AlterStatementSuffixTouchContext) {
}

// ExitAlterStatementSuffixTouch is called when production alterStatementSuffixTouch is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixTouch(ctx *AlterStatementSuffixTouchContext) {
}

// EnterAlterStatementSuffixArchive is called when production alterStatementSuffixArchive is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixArchive(ctx *AlterStatementSuffixArchiveContext) {
}

// ExitAlterStatementSuffixArchive is called when production alterStatementSuffixArchive is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixArchive(ctx *AlterStatementSuffixArchiveContext) {
}

// EnterAlterStatementSuffixUnArchive is called when production alterStatementSuffixUnArchive is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixUnArchive(ctx *AlterStatementSuffixUnArchiveContext) {
}

// ExitAlterStatementSuffixUnArchive is called when production alterStatementSuffixUnArchive is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixUnArchive(ctx *AlterStatementSuffixUnArchiveContext) {
}

// EnterAlterStatementSuffixChangeOwner is called when production alterStatementSuffixChangeOwner is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixChangeOwner(ctx *AlterStatementSuffixChangeOwnerContext) {
}

// ExitAlterStatementSuffixChangeOwner is called when production alterStatementSuffixChangeOwner is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixChangeOwner(ctx *AlterStatementSuffixChangeOwnerContext) {
}

// EnterPartitionLocation is called when production partitionLocation is entered.
func (s *BaseOdpsParserListener) EnterPartitionLocation(ctx *PartitionLocationContext) {}

// ExitPartitionLocation is called when production partitionLocation is exited.
func (s *BaseOdpsParserListener) ExitPartitionLocation(ctx *PartitionLocationContext) {}

// EnterAlterStatementSuffixDropPartitions is called when production alterStatementSuffixDropPartitions is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixDropPartitions(ctx *AlterStatementSuffixDropPartitionsContext) {
}

// ExitAlterStatementSuffixDropPartitions is called when production alterStatementSuffixDropPartitions is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixDropPartitions(ctx *AlterStatementSuffixDropPartitionsContext) {
}

// EnterAlterStatementSuffixProperties is called when production alterStatementSuffixProperties is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixProperties(ctx *AlterStatementSuffixPropertiesContext) {
}

// ExitAlterStatementSuffixProperties is called when production alterStatementSuffixProperties is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixProperties(ctx *AlterStatementSuffixPropertiesContext) {
}

// EnterAlterViewSuffixProperties is called when production alterViewSuffixProperties is entered.
func (s *BaseOdpsParserListener) EnterAlterViewSuffixProperties(ctx *AlterViewSuffixPropertiesContext) {
}

// ExitAlterViewSuffixProperties is called when production alterViewSuffixProperties is exited.
func (s *BaseOdpsParserListener) ExitAlterViewSuffixProperties(ctx *AlterViewSuffixPropertiesContext) {
}

// EnterAlterViewColumnCommentSuffix is called when production alterViewColumnCommentSuffix is entered.
func (s *BaseOdpsParserListener) EnterAlterViewColumnCommentSuffix(ctx *AlterViewColumnCommentSuffixContext) {
}

// ExitAlterViewColumnCommentSuffix is called when production alterViewColumnCommentSuffix is exited.
func (s *BaseOdpsParserListener) ExitAlterViewColumnCommentSuffix(ctx *AlterViewColumnCommentSuffixContext) {
}

// EnterAlterStatementSuffixSerdeProperties is called when production alterStatementSuffixSerdeProperties is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixSerdeProperties(ctx *AlterStatementSuffixSerdePropertiesContext) {
}

// ExitAlterStatementSuffixSerdeProperties is called when production alterStatementSuffixSerdeProperties is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixSerdeProperties(ctx *AlterStatementSuffixSerdePropertiesContext) {
}

// EnterTablePartitionPrefix is called when production tablePartitionPrefix is entered.
func (s *BaseOdpsParserListener) EnterTablePartitionPrefix(ctx *TablePartitionPrefixContext) {}

// ExitTablePartitionPrefix is called when production tablePartitionPrefix is exited.
func (s *BaseOdpsParserListener) ExitTablePartitionPrefix(ctx *TablePartitionPrefixContext) {}

// EnterAlterStatementSuffixFileFormat is called when production alterStatementSuffixFileFormat is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixFileFormat(ctx *AlterStatementSuffixFileFormatContext) {
}

// ExitAlterStatementSuffixFileFormat is called when production alterStatementSuffixFileFormat is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixFileFormat(ctx *AlterStatementSuffixFileFormatContext) {
}

// EnterAlterStatementSuffixClusterbySortby is called when production alterStatementSuffixClusterbySortby is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixClusterbySortby(ctx *AlterStatementSuffixClusterbySortbyContext) {
}

// ExitAlterStatementSuffixClusterbySortby is called when production alterStatementSuffixClusterbySortby is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixClusterbySortby(ctx *AlterStatementSuffixClusterbySortbyContext) {
}

// EnterAlterTblPartitionStatementSuffixSkewedLocation is called when production alterTblPartitionStatementSuffixSkewedLocation is entered.
func (s *BaseOdpsParserListener) EnterAlterTblPartitionStatementSuffixSkewedLocation(ctx *AlterTblPartitionStatementSuffixSkewedLocationContext) {
}

// ExitAlterTblPartitionStatementSuffixSkewedLocation is called when production alterTblPartitionStatementSuffixSkewedLocation is exited.
func (s *BaseOdpsParserListener) ExitAlterTblPartitionStatementSuffixSkewedLocation(ctx *AlterTblPartitionStatementSuffixSkewedLocationContext) {
}

// EnterSkewedLocations is called when production skewedLocations is entered.
func (s *BaseOdpsParserListener) EnterSkewedLocations(ctx *SkewedLocationsContext) {}

// ExitSkewedLocations is called when production skewedLocations is exited.
func (s *BaseOdpsParserListener) ExitSkewedLocations(ctx *SkewedLocationsContext) {}

// EnterSkewedLocationsList is called when production skewedLocationsList is entered.
func (s *BaseOdpsParserListener) EnterSkewedLocationsList(ctx *SkewedLocationsListContext) {}

// ExitSkewedLocationsList is called when production skewedLocationsList is exited.
func (s *BaseOdpsParserListener) ExitSkewedLocationsList(ctx *SkewedLocationsListContext) {}

// EnterSkewedLocationMap is called when production skewedLocationMap is entered.
func (s *BaseOdpsParserListener) EnterSkewedLocationMap(ctx *SkewedLocationMapContext) {}

// ExitSkewedLocationMap is called when production skewedLocationMap is exited.
func (s *BaseOdpsParserListener) ExitSkewedLocationMap(ctx *SkewedLocationMapContext) {}

// EnterAlterStatementSuffixLocation is called when production alterStatementSuffixLocation is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixLocation(ctx *AlterStatementSuffixLocationContext) {
}

// ExitAlterStatementSuffixLocation is called when production alterStatementSuffixLocation is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixLocation(ctx *AlterStatementSuffixLocationContext) {
}

// EnterAlterStatementSuffixSkewedby is called when production alterStatementSuffixSkewedby is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixSkewedby(ctx *AlterStatementSuffixSkewedbyContext) {
}

// ExitAlterStatementSuffixSkewedby is called when production alterStatementSuffixSkewedby is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixSkewedby(ctx *AlterStatementSuffixSkewedbyContext) {
}

// EnterAlterStatementSuffixExchangePartition is called when production alterStatementSuffixExchangePartition is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixExchangePartition(ctx *AlterStatementSuffixExchangePartitionContext) {
}

// ExitAlterStatementSuffixExchangePartition is called when production alterStatementSuffixExchangePartition is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixExchangePartition(ctx *AlterStatementSuffixExchangePartitionContext) {
}

// EnterAlterStatementSuffixProtectMode is called when production alterStatementSuffixProtectMode is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixProtectMode(ctx *AlterStatementSuffixProtectModeContext) {
}

// ExitAlterStatementSuffixProtectMode is called when production alterStatementSuffixProtectMode is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixProtectMode(ctx *AlterStatementSuffixProtectModeContext) {
}

// EnterAlterStatementSuffixRenamePart is called when production alterStatementSuffixRenamePart is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixRenamePart(ctx *AlterStatementSuffixRenamePartContext) {
}

// ExitAlterStatementSuffixRenamePart is called when production alterStatementSuffixRenamePart is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixRenamePart(ctx *AlterStatementSuffixRenamePartContext) {
}

// EnterAlterStatementSuffixStatsPart is called when production alterStatementSuffixStatsPart is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixStatsPart(ctx *AlterStatementSuffixStatsPartContext) {
}

// ExitAlterStatementSuffixStatsPart is called when production alterStatementSuffixStatsPart is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixStatsPart(ctx *AlterStatementSuffixStatsPartContext) {
}

// EnterAlterStatementSuffixMergeFiles is called when production alterStatementSuffixMergeFiles is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixMergeFiles(ctx *AlterStatementSuffixMergeFilesContext) {
}

// ExitAlterStatementSuffixMergeFiles is called when production alterStatementSuffixMergeFiles is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixMergeFiles(ctx *AlterStatementSuffixMergeFilesContext) {
}

// EnterAlterProtectMode is called when production alterProtectMode is entered.
func (s *BaseOdpsParserListener) EnterAlterProtectMode(ctx *AlterProtectModeContext) {}

// ExitAlterProtectMode is called when production alterProtectMode is exited.
func (s *BaseOdpsParserListener) ExitAlterProtectMode(ctx *AlterProtectModeContext) {}

// EnterAlterProtectModeMode is called when production alterProtectModeMode is entered.
func (s *BaseOdpsParserListener) EnterAlterProtectModeMode(ctx *AlterProtectModeModeContext) {}

// ExitAlterProtectModeMode is called when production alterProtectModeMode is exited.
func (s *BaseOdpsParserListener) ExitAlterProtectModeMode(ctx *AlterProtectModeModeContext) {}

// EnterAlterStatementSuffixBucketNum is called when production alterStatementSuffixBucketNum is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixBucketNum(ctx *AlterStatementSuffixBucketNumContext) {
}

// ExitAlterStatementSuffixBucketNum is called when production alterStatementSuffixBucketNum is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixBucketNum(ctx *AlterStatementSuffixBucketNumContext) {
}

// EnterAlterStatementSuffixCompact is called when production alterStatementSuffixCompact is entered.
func (s *BaseOdpsParserListener) EnterAlterStatementSuffixCompact(ctx *AlterStatementSuffixCompactContext) {
}

// ExitAlterStatementSuffixCompact is called when production alterStatementSuffixCompact is exited.
func (s *BaseOdpsParserListener) ExitAlterStatementSuffixCompact(ctx *AlterStatementSuffixCompactContext) {
}

// EnterFileFormat is called when production fileFormat is entered.
func (s *BaseOdpsParserListener) EnterFileFormat(ctx *FileFormatContext) {}

// ExitFileFormat is called when production fileFormat is exited.
func (s *BaseOdpsParserListener) ExitFileFormat(ctx *FileFormatContext) {}

// EnterTabTypeExpr is called when production tabTypeExpr is entered.
func (s *BaseOdpsParserListener) EnterTabTypeExpr(ctx *TabTypeExprContext) {}

// ExitTabTypeExpr is called when production tabTypeExpr is exited.
func (s *BaseOdpsParserListener) ExitTabTypeExpr(ctx *TabTypeExprContext) {}

// EnterPartTypeExpr is called when production partTypeExpr is entered.
func (s *BaseOdpsParserListener) EnterPartTypeExpr(ctx *PartTypeExprContext) {}

// ExitPartTypeExpr is called when production partTypeExpr is exited.
func (s *BaseOdpsParserListener) ExitPartTypeExpr(ctx *PartTypeExprContext) {}

// EnterDescStatement is called when production descStatement is entered.
func (s *BaseOdpsParserListener) EnterDescStatement(ctx *DescStatementContext) {}

// ExitDescStatement is called when production descStatement is exited.
func (s *BaseOdpsParserListener) ExitDescStatement(ctx *DescStatementContext) {}

// EnterAnalyzeStatement is called when production analyzeStatement is entered.
func (s *BaseOdpsParserListener) EnterAnalyzeStatement(ctx *AnalyzeStatementContext) {}

// ExitAnalyzeStatement is called when production analyzeStatement is exited.
func (s *BaseOdpsParserListener) ExitAnalyzeStatement(ctx *AnalyzeStatementContext) {}

// EnterForColumnsStatement is called when production forColumnsStatement is entered.
func (s *BaseOdpsParserListener) EnterForColumnsStatement(ctx *ForColumnsStatementContext) {}

// ExitForColumnsStatement is called when production forColumnsStatement is exited.
func (s *BaseOdpsParserListener) ExitForColumnsStatement(ctx *ForColumnsStatementContext) {}

// EnterColumnNameOrList is called when production columnNameOrList is entered.
func (s *BaseOdpsParserListener) EnterColumnNameOrList(ctx *ColumnNameOrListContext) {}

// ExitColumnNameOrList is called when production columnNameOrList is exited.
func (s *BaseOdpsParserListener) ExitColumnNameOrList(ctx *ColumnNameOrListContext) {}

// EnterShowStatement is called when production showStatement is entered.
func (s *BaseOdpsParserListener) EnterShowStatement(ctx *ShowStatementContext) {}

// ExitShowStatement is called when production showStatement is exited.
func (s *BaseOdpsParserListener) ExitShowStatement(ctx *ShowStatementContext) {}

// EnterListStatement is called when production listStatement is entered.
func (s *BaseOdpsParserListener) EnterListStatement(ctx *ListStatementContext) {}

// ExitListStatement is called when production listStatement is exited.
func (s *BaseOdpsParserListener) ExitListStatement(ctx *ListStatementContext) {}

// EnterBareDate is called when production bareDate is entered.
func (s *BaseOdpsParserListener) EnterBareDate(ctx *BareDateContext) {}

// ExitBareDate is called when production bareDate is exited.
func (s *BaseOdpsParserListener) ExitBareDate(ctx *BareDateContext) {}

// EnterLockStatement is called when production lockStatement is entered.
func (s *BaseOdpsParserListener) EnterLockStatement(ctx *LockStatementContext) {}

// ExitLockStatement is called when production lockStatement is exited.
func (s *BaseOdpsParserListener) ExitLockStatement(ctx *LockStatementContext) {}

// EnterLockDatabase is called when production lockDatabase is entered.
func (s *BaseOdpsParserListener) EnterLockDatabase(ctx *LockDatabaseContext) {}

// ExitLockDatabase is called when production lockDatabase is exited.
func (s *BaseOdpsParserListener) ExitLockDatabase(ctx *LockDatabaseContext) {}

// EnterLockMode is called when production lockMode is entered.
func (s *BaseOdpsParserListener) EnterLockMode(ctx *LockModeContext) {}

// ExitLockMode is called when production lockMode is exited.
func (s *BaseOdpsParserListener) ExitLockMode(ctx *LockModeContext) {}

// EnterUnlockStatement is called when production unlockStatement is entered.
func (s *BaseOdpsParserListener) EnterUnlockStatement(ctx *UnlockStatementContext) {}

// ExitUnlockStatement is called when production unlockStatement is exited.
func (s *BaseOdpsParserListener) ExitUnlockStatement(ctx *UnlockStatementContext) {}

// EnterUnlockDatabase is called when production unlockDatabase is entered.
func (s *BaseOdpsParserListener) EnterUnlockDatabase(ctx *UnlockDatabaseContext) {}

// ExitUnlockDatabase is called when production unlockDatabase is exited.
func (s *BaseOdpsParserListener) ExitUnlockDatabase(ctx *UnlockDatabaseContext) {}

// EnterResourceList is called when production resourceList is entered.
func (s *BaseOdpsParserListener) EnterResourceList(ctx *ResourceListContext) {}

// ExitResourceList is called when production resourceList is exited.
func (s *BaseOdpsParserListener) ExitResourceList(ctx *ResourceListContext) {}

// EnterResource is called when production resource is entered.
func (s *BaseOdpsParserListener) EnterResource(ctx *ResourceContext) {}

// ExitResource is called when production resource is exited.
func (s *BaseOdpsParserListener) ExitResource(ctx *ResourceContext) {}

// EnterResourceType is called when production resourceType is entered.
func (s *BaseOdpsParserListener) EnterResourceType(ctx *ResourceTypeContext) {}

// ExitResourceType is called when production resourceType is exited.
func (s *BaseOdpsParserListener) ExitResourceType(ctx *ResourceTypeContext) {}

// EnterCreateFunctionStatement is called when production createFunctionStatement is entered.
func (s *BaseOdpsParserListener) EnterCreateFunctionStatement(ctx *CreateFunctionStatementContext) {}

// ExitCreateFunctionStatement is called when production createFunctionStatement is exited.
func (s *BaseOdpsParserListener) ExitCreateFunctionStatement(ctx *CreateFunctionStatementContext) {}

// EnterDropFunctionStatement is called when production dropFunctionStatement is entered.
func (s *BaseOdpsParserListener) EnterDropFunctionStatement(ctx *DropFunctionStatementContext) {}

// ExitDropFunctionStatement is called when production dropFunctionStatement is exited.
func (s *BaseOdpsParserListener) ExitDropFunctionStatement(ctx *DropFunctionStatementContext) {}

// EnterReloadFunctionStatement is called when production reloadFunctionStatement is entered.
func (s *BaseOdpsParserListener) EnterReloadFunctionStatement(ctx *ReloadFunctionStatementContext) {}

// ExitReloadFunctionStatement is called when production reloadFunctionStatement is exited.
func (s *BaseOdpsParserListener) ExitReloadFunctionStatement(ctx *ReloadFunctionStatementContext) {}

// EnterCreateMacroStatement is called when production createMacroStatement is entered.
func (s *BaseOdpsParserListener) EnterCreateMacroStatement(ctx *CreateMacroStatementContext) {}

// ExitCreateMacroStatement is called when production createMacroStatement is exited.
func (s *BaseOdpsParserListener) ExitCreateMacroStatement(ctx *CreateMacroStatementContext) {}

// EnterDropMacroStatement is called when production dropMacroStatement is entered.
func (s *BaseOdpsParserListener) EnterDropMacroStatement(ctx *DropMacroStatementContext) {}

// ExitDropMacroStatement is called when production dropMacroStatement is exited.
func (s *BaseOdpsParserListener) ExitDropMacroStatement(ctx *DropMacroStatementContext) {}

// EnterCreateSqlFunctionStatement is called when production createSqlFunctionStatement is entered.
func (s *BaseOdpsParserListener) EnterCreateSqlFunctionStatement(ctx *CreateSqlFunctionStatementContext) {
}

// ExitCreateSqlFunctionStatement is called when production createSqlFunctionStatement is exited.
func (s *BaseOdpsParserListener) ExitCreateSqlFunctionStatement(ctx *CreateSqlFunctionStatementContext) {
}

// EnterCloneTableStatement is called when production cloneTableStatement is entered.
func (s *BaseOdpsParserListener) EnterCloneTableStatement(ctx *CloneTableStatementContext) {}

// ExitCloneTableStatement is called when production cloneTableStatement is exited.
func (s *BaseOdpsParserListener) ExitCloneTableStatement(ctx *CloneTableStatementContext) {}

// EnterCreateViewStatement is called when production createViewStatement is entered.
func (s *BaseOdpsParserListener) EnterCreateViewStatement(ctx *CreateViewStatementContext) {}

// ExitCreateViewStatement is called when production createViewStatement is exited.
func (s *BaseOdpsParserListener) ExitCreateViewStatement(ctx *CreateViewStatementContext) {}

// EnterViewPartition is called when production viewPartition is entered.
func (s *BaseOdpsParserListener) EnterViewPartition(ctx *ViewPartitionContext) {}

// ExitViewPartition is called when production viewPartition is exited.
func (s *BaseOdpsParserListener) ExitViewPartition(ctx *ViewPartitionContext) {}

// EnterDropViewStatement is called when production dropViewStatement is entered.
func (s *BaseOdpsParserListener) EnterDropViewStatement(ctx *DropViewStatementContext) {}

// ExitDropViewStatement is called when production dropViewStatement is exited.
func (s *BaseOdpsParserListener) ExitDropViewStatement(ctx *DropViewStatementContext) {}

// EnterCreateMaterializedViewStatement is called when production createMaterializedViewStatement is entered.
func (s *BaseOdpsParserListener) EnterCreateMaterializedViewStatement(ctx *CreateMaterializedViewStatementContext) {
}

// ExitCreateMaterializedViewStatement is called when production createMaterializedViewStatement is exited.
func (s *BaseOdpsParserListener) ExitCreateMaterializedViewStatement(ctx *CreateMaterializedViewStatementContext) {
}

// EnterDropMaterializedViewStatement is called when production dropMaterializedViewStatement is entered.
func (s *BaseOdpsParserListener) EnterDropMaterializedViewStatement(ctx *DropMaterializedViewStatementContext) {
}

// ExitDropMaterializedViewStatement is called when production dropMaterializedViewStatement is exited.
func (s *BaseOdpsParserListener) ExitDropMaterializedViewStatement(ctx *DropMaterializedViewStatementContext) {
}

// EnterShowFunctionIdentifier is called when production showFunctionIdentifier is entered.
func (s *BaseOdpsParserListener) EnterShowFunctionIdentifier(ctx *ShowFunctionIdentifierContext) {}

// ExitShowFunctionIdentifier is called when production showFunctionIdentifier is exited.
func (s *BaseOdpsParserListener) ExitShowFunctionIdentifier(ctx *ShowFunctionIdentifierContext) {}

// EnterShowStmtIdentifier is called when production showStmtIdentifier is entered.
func (s *BaseOdpsParserListener) EnterShowStmtIdentifier(ctx *ShowStmtIdentifierContext) {}

// ExitShowStmtIdentifier is called when production showStmtIdentifier is exited.
func (s *BaseOdpsParserListener) ExitShowStmtIdentifier(ctx *ShowStmtIdentifierContext) {}

// EnterTableComment is called when production tableComment is entered.
func (s *BaseOdpsParserListener) EnterTableComment(ctx *TableCommentContext) {}

// ExitTableComment is called when production tableComment is exited.
func (s *BaseOdpsParserListener) ExitTableComment(ctx *TableCommentContext) {}

// EnterTablePartition is called when production tablePartition is entered.
func (s *BaseOdpsParserListener) EnterTablePartition(ctx *TablePartitionContext) {}

// ExitTablePartition is called when production tablePartition is exited.
func (s *BaseOdpsParserListener) ExitTablePartition(ctx *TablePartitionContext) {}

// EnterTableBuckets is called when production tableBuckets is entered.
func (s *BaseOdpsParserListener) EnterTableBuckets(ctx *TableBucketsContext) {}

// ExitTableBuckets is called when production tableBuckets is exited.
func (s *BaseOdpsParserListener) ExitTableBuckets(ctx *TableBucketsContext) {}

// EnterTableShards is called when production tableShards is entered.
func (s *BaseOdpsParserListener) EnterTableShards(ctx *TableShardsContext) {}

// ExitTableShards is called when production tableShards is exited.
func (s *BaseOdpsParserListener) ExitTableShards(ctx *TableShardsContext) {}

// EnterTableSkewed is called when production tableSkewed is entered.
func (s *BaseOdpsParserListener) EnterTableSkewed(ctx *TableSkewedContext) {}

// ExitTableSkewed is called when production tableSkewed is exited.
func (s *BaseOdpsParserListener) ExitTableSkewed(ctx *TableSkewedContext) {}

// EnterRowFormat is called when production rowFormat is entered.
func (s *BaseOdpsParserListener) EnterRowFormat(ctx *RowFormatContext) {}

// ExitRowFormat is called when production rowFormat is exited.
func (s *BaseOdpsParserListener) ExitRowFormat(ctx *RowFormatContext) {}

// EnterRecordReader is called when production recordReader is entered.
func (s *BaseOdpsParserListener) EnterRecordReader(ctx *RecordReaderContext) {}

// ExitRecordReader is called when production recordReader is exited.
func (s *BaseOdpsParserListener) ExitRecordReader(ctx *RecordReaderContext) {}

// EnterRecordWriter is called when production recordWriter is entered.
func (s *BaseOdpsParserListener) EnterRecordWriter(ctx *RecordWriterContext) {}

// ExitRecordWriter is called when production recordWriter is exited.
func (s *BaseOdpsParserListener) ExitRecordWriter(ctx *RecordWriterContext) {}

// EnterRowFormatSerde is called when production rowFormatSerde is entered.
func (s *BaseOdpsParserListener) EnterRowFormatSerde(ctx *RowFormatSerdeContext) {}

// ExitRowFormatSerde is called when production rowFormatSerde is exited.
func (s *BaseOdpsParserListener) ExitRowFormatSerde(ctx *RowFormatSerdeContext) {}

// EnterRowFormatDelimited is called when production rowFormatDelimited is entered.
func (s *BaseOdpsParserListener) EnterRowFormatDelimited(ctx *RowFormatDelimitedContext) {}

// ExitRowFormatDelimited is called when production rowFormatDelimited is exited.
func (s *BaseOdpsParserListener) ExitRowFormatDelimited(ctx *RowFormatDelimitedContext) {}

// EnterTableRowFormat is called when production tableRowFormat is entered.
func (s *BaseOdpsParserListener) EnterTableRowFormat(ctx *TableRowFormatContext) {}

// ExitTableRowFormat is called when production tableRowFormat is exited.
func (s *BaseOdpsParserListener) ExitTableRowFormat(ctx *TableRowFormatContext) {}

// EnterTablePropertiesPrefixed is called when production tablePropertiesPrefixed is entered.
func (s *BaseOdpsParserListener) EnterTablePropertiesPrefixed(ctx *TablePropertiesPrefixedContext) {}

// ExitTablePropertiesPrefixed is called when production tablePropertiesPrefixed is exited.
func (s *BaseOdpsParserListener) ExitTablePropertiesPrefixed(ctx *TablePropertiesPrefixedContext) {}

// EnterTableProperties is called when production tableProperties is entered.
func (s *BaseOdpsParserListener) EnterTableProperties(ctx *TablePropertiesContext) {}

// ExitTableProperties is called when production tableProperties is exited.
func (s *BaseOdpsParserListener) ExitTableProperties(ctx *TablePropertiesContext) {}

// EnterTablePropertiesList is called when production tablePropertiesList is entered.
func (s *BaseOdpsParserListener) EnterTablePropertiesList(ctx *TablePropertiesListContext) {}

// ExitTablePropertiesList is called when production tablePropertiesList is exited.
func (s *BaseOdpsParserListener) ExitTablePropertiesList(ctx *TablePropertiesListContext) {}

// EnterKeyValueProperty is called when production keyValueProperty is entered.
func (s *BaseOdpsParserListener) EnterKeyValueProperty(ctx *KeyValuePropertyContext) {}

// ExitKeyValueProperty is called when production keyValueProperty is exited.
func (s *BaseOdpsParserListener) ExitKeyValueProperty(ctx *KeyValuePropertyContext) {}

// EnterUserDefinedJoinPropertiesList is called when production userDefinedJoinPropertiesList is entered.
func (s *BaseOdpsParserListener) EnterUserDefinedJoinPropertiesList(ctx *UserDefinedJoinPropertiesListContext) {
}

// ExitUserDefinedJoinPropertiesList is called when production userDefinedJoinPropertiesList is exited.
func (s *BaseOdpsParserListener) ExitUserDefinedJoinPropertiesList(ctx *UserDefinedJoinPropertiesListContext) {
}

// EnterKeyPrivProperty is called when production keyPrivProperty is entered.
func (s *BaseOdpsParserListener) EnterKeyPrivProperty(ctx *KeyPrivPropertyContext) {}

// ExitKeyPrivProperty is called when production keyPrivProperty is exited.
func (s *BaseOdpsParserListener) ExitKeyPrivProperty(ctx *KeyPrivPropertyContext) {}

// EnterKeyProperty is called when production keyProperty is entered.
func (s *BaseOdpsParserListener) EnterKeyProperty(ctx *KeyPropertyContext) {}

// ExitKeyProperty is called when production keyProperty is exited.
func (s *BaseOdpsParserListener) ExitKeyProperty(ctx *KeyPropertyContext) {}

// EnterTableRowFormatFieldIdentifier is called when production tableRowFormatFieldIdentifier is entered.
func (s *BaseOdpsParserListener) EnterTableRowFormatFieldIdentifier(ctx *TableRowFormatFieldIdentifierContext) {
}

// ExitTableRowFormatFieldIdentifier is called when production tableRowFormatFieldIdentifier is exited.
func (s *BaseOdpsParserListener) ExitTableRowFormatFieldIdentifier(ctx *TableRowFormatFieldIdentifierContext) {
}

// EnterTableRowFormatCollItemsIdentifier is called when production tableRowFormatCollItemsIdentifier is entered.
func (s *BaseOdpsParserListener) EnterTableRowFormatCollItemsIdentifier(ctx *TableRowFormatCollItemsIdentifierContext) {
}

// ExitTableRowFormatCollItemsIdentifier is called when production tableRowFormatCollItemsIdentifier is exited.
func (s *BaseOdpsParserListener) ExitTableRowFormatCollItemsIdentifier(ctx *TableRowFormatCollItemsIdentifierContext) {
}

// EnterTableRowFormatMapKeysIdentifier is called when production tableRowFormatMapKeysIdentifier is entered.
func (s *BaseOdpsParserListener) EnterTableRowFormatMapKeysIdentifier(ctx *TableRowFormatMapKeysIdentifierContext) {
}

// ExitTableRowFormatMapKeysIdentifier is called when production tableRowFormatMapKeysIdentifier is exited.
func (s *BaseOdpsParserListener) ExitTableRowFormatMapKeysIdentifier(ctx *TableRowFormatMapKeysIdentifierContext) {
}

// EnterTableRowFormatLinesIdentifier is called when production tableRowFormatLinesIdentifier is entered.
func (s *BaseOdpsParserListener) EnterTableRowFormatLinesIdentifier(ctx *TableRowFormatLinesIdentifierContext) {
}

// ExitTableRowFormatLinesIdentifier is called when production tableRowFormatLinesIdentifier is exited.
func (s *BaseOdpsParserListener) ExitTableRowFormatLinesIdentifier(ctx *TableRowFormatLinesIdentifierContext) {
}

// EnterTableRowNullFormat is called when production tableRowNullFormat is entered.
func (s *BaseOdpsParserListener) EnterTableRowNullFormat(ctx *TableRowNullFormatContext) {}

// ExitTableRowNullFormat is called when production tableRowNullFormat is exited.
func (s *BaseOdpsParserListener) ExitTableRowNullFormat(ctx *TableRowNullFormatContext) {}

// EnterTableFileFormat is called when production tableFileFormat is entered.
func (s *BaseOdpsParserListener) EnterTableFileFormat(ctx *TableFileFormatContext) {}

// ExitTableFileFormat is called when production tableFileFormat is exited.
func (s *BaseOdpsParserListener) ExitTableFileFormat(ctx *TableFileFormatContext) {}

// EnterTableLocation is called when production tableLocation is entered.
func (s *BaseOdpsParserListener) EnterTableLocation(ctx *TableLocationContext) {}

// ExitTableLocation is called when production tableLocation is exited.
func (s *BaseOdpsParserListener) ExitTableLocation(ctx *TableLocationContext) {}

// EnterExternalTableResource is called when production externalTableResource is entered.
func (s *BaseOdpsParserListener) EnterExternalTableResource(ctx *ExternalTableResourceContext) {}

// ExitExternalTableResource is called when production externalTableResource is exited.
func (s *BaseOdpsParserListener) ExitExternalTableResource(ctx *ExternalTableResourceContext) {}

// EnterViewResource is called when production viewResource is entered.
func (s *BaseOdpsParserListener) EnterViewResource(ctx *ViewResourceContext) {}

// ExitViewResource is called when production viewResource is exited.
func (s *BaseOdpsParserListener) ExitViewResource(ctx *ViewResourceContext) {}

// EnterOutOfLineConstraints is called when production outOfLineConstraints is entered.
func (s *BaseOdpsParserListener) EnterOutOfLineConstraints(ctx *OutOfLineConstraintsContext) {}

// ExitOutOfLineConstraints is called when production outOfLineConstraints is exited.
func (s *BaseOdpsParserListener) ExitOutOfLineConstraints(ctx *OutOfLineConstraintsContext) {}

// EnterEnableSpec is called when production enableSpec is entered.
func (s *BaseOdpsParserListener) EnterEnableSpec(ctx *EnableSpecContext) {}

// ExitEnableSpec is called when production enableSpec is exited.
func (s *BaseOdpsParserListener) ExitEnableSpec(ctx *EnableSpecContext) {}

// EnterValidateSpec is called when production validateSpec is entered.
func (s *BaseOdpsParserListener) EnterValidateSpec(ctx *ValidateSpecContext) {}

// ExitValidateSpec is called when production validateSpec is exited.
func (s *BaseOdpsParserListener) ExitValidateSpec(ctx *ValidateSpecContext) {}

// EnterRelySpec is called when production relySpec is entered.
func (s *BaseOdpsParserListener) EnterRelySpec(ctx *RelySpecContext) {}

// ExitRelySpec is called when production relySpec is exited.
func (s *BaseOdpsParserListener) ExitRelySpec(ctx *RelySpecContext) {}

// EnterColumnNameTypeConstraintList is called when production columnNameTypeConstraintList is entered.
func (s *BaseOdpsParserListener) EnterColumnNameTypeConstraintList(ctx *ColumnNameTypeConstraintListContext) {
}

// ExitColumnNameTypeConstraintList is called when production columnNameTypeConstraintList is exited.
func (s *BaseOdpsParserListener) ExitColumnNameTypeConstraintList(ctx *ColumnNameTypeConstraintListContext) {
}

// EnterColumnNameTypeList is called when production columnNameTypeList is entered.
func (s *BaseOdpsParserListener) EnterColumnNameTypeList(ctx *ColumnNameTypeListContext) {}

// ExitColumnNameTypeList is called when production columnNameTypeList is exited.
func (s *BaseOdpsParserListener) ExitColumnNameTypeList(ctx *ColumnNameTypeListContext) {}

// EnterPartitionColumnNameTypeList is called when production partitionColumnNameTypeList is entered.
func (s *BaseOdpsParserListener) EnterPartitionColumnNameTypeList(ctx *PartitionColumnNameTypeListContext) {
}

// ExitPartitionColumnNameTypeList is called when production partitionColumnNameTypeList is exited.
func (s *BaseOdpsParserListener) ExitPartitionColumnNameTypeList(ctx *PartitionColumnNameTypeListContext) {
}

// EnterColumnNameTypeConstraintWithPosList is called when production columnNameTypeConstraintWithPosList is entered.
func (s *BaseOdpsParserListener) EnterColumnNameTypeConstraintWithPosList(ctx *ColumnNameTypeConstraintWithPosListContext) {
}

// ExitColumnNameTypeConstraintWithPosList is called when production columnNameTypeConstraintWithPosList is exited.
func (s *BaseOdpsParserListener) ExitColumnNameTypeConstraintWithPosList(ctx *ColumnNameTypeConstraintWithPosListContext) {
}

// EnterColumnNameColonTypeList is called when production columnNameColonTypeList is entered.
func (s *BaseOdpsParserListener) EnterColumnNameColonTypeList(ctx *ColumnNameColonTypeListContext) {}

// ExitColumnNameColonTypeList is called when production columnNameColonTypeList is exited.
func (s *BaseOdpsParserListener) ExitColumnNameColonTypeList(ctx *ColumnNameColonTypeListContext) {}

// EnterColumnNameList is called when production columnNameList is entered.
func (s *BaseOdpsParserListener) EnterColumnNameList(ctx *ColumnNameListContext) {}

// ExitColumnNameList is called when production columnNameList is exited.
func (s *BaseOdpsParserListener) ExitColumnNameList(ctx *ColumnNameListContext) {}

// EnterColumnNameListInParentheses is called when production columnNameListInParentheses is entered.
func (s *BaseOdpsParserListener) EnterColumnNameListInParentheses(ctx *ColumnNameListInParenthesesContext) {
}

// ExitColumnNameListInParentheses is called when production columnNameListInParentheses is exited.
func (s *BaseOdpsParserListener) ExitColumnNameListInParentheses(ctx *ColumnNameListInParenthesesContext) {
}

// EnterColumnName is called when production columnName is entered.
func (s *BaseOdpsParserListener) EnterColumnName(ctx *ColumnNameContext) {}

// ExitColumnName is called when production columnName is exited.
func (s *BaseOdpsParserListener) ExitColumnName(ctx *ColumnNameContext) {}

// EnterColumnNameOrderList is called when production columnNameOrderList is entered.
func (s *BaseOdpsParserListener) EnterColumnNameOrderList(ctx *ColumnNameOrderListContext) {}

// ExitColumnNameOrderList is called when production columnNameOrderList is exited.
func (s *BaseOdpsParserListener) ExitColumnNameOrderList(ctx *ColumnNameOrderListContext) {}

// EnterClusterColumnNameOrderList is called when production clusterColumnNameOrderList is entered.
func (s *BaseOdpsParserListener) EnterClusterColumnNameOrderList(ctx *ClusterColumnNameOrderListContext) {
}

// ExitClusterColumnNameOrderList is called when production clusterColumnNameOrderList is exited.
func (s *BaseOdpsParserListener) ExitClusterColumnNameOrderList(ctx *ClusterColumnNameOrderListContext) {
}

// EnterSkewedValueElement is called when production skewedValueElement is entered.
func (s *BaseOdpsParserListener) EnterSkewedValueElement(ctx *SkewedValueElementContext) {}

// ExitSkewedValueElement is called when production skewedValueElement is exited.
func (s *BaseOdpsParserListener) ExitSkewedValueElement(ctx *SkewedValueElementContext) {}

// EnterSkewedColumnValuePairList is called when production skewedColumnValuePairList is entered.
func (s *BaseOdpsParserListener) EnterSkewedColumnValuePairList(ctx *SkewedColumnValuePairListContext) {
}

// ExitSkewedColumnValuePairList is called when production skewedColumnValuePairList is exited.
func (s *BaseOdpsParserListener) ExitSkewedColumnValuePairList(ctx *SkewedColumnValuePairListContext) {
}

// EnterSkewedColumnValuePair is called when production skewedColumnValuePair is entered.
func (s *BaseOdpsParserListener) EnterSkewedColumnValuePair(ctx *SkewedColumnValuePairContext) {}

// ExitSkewedColumnValuePair is called when production skewedColumnValuePair is exited.
func (s *BaseOdpsParserListener) ExitSkewedColumnValuePair(ctx *SkewedColumnValuePairContext) {}

// EnterSkewedColumnValues is called when production skewedColumnValues is entered.
func (s *BaseOdpsParserListener) EnterSkewedColumnValues(ctx *SkewedColumnValuesContext) {}

// ExitSkewedColumnValues is called when production skewedColumnValues is exited.
func (s *BaseOdpsParserListener) ExitSkewedColumnValues(ctx *SkewedColumnValuesContext) {}

// EnterSkewedColumnValue is called when production skewedColumnValue is entered.
func (s *BaseOdpsParserListener) EnterSkewedColumnValue(ctx *SkewedColumnValueContext) {}

// ExitSkewedColumnValue is called when production skewedColumnValue is exited.
func (s *BaseOdpsParserListener) ExitSkewedColumnValue(ctx *SkewedColumnValueContext) {}

// EnterSkewedValueLocationElement is called when production skewedValueLocationElement is entered.
func (s *BaseOdpsParserListener) EnterSkewedValueLocationElement(ctx *SkewedValueLocationElementContext) {
}

// ExitSkewedValueLocationElement is called when production skewedValueLocationElement is exited.
func (s *BaseOdpsParserListener) ExitSkewedValueLocationElement(ctx *SkewedValueLocationElementContext) {
}

// EnterColumnNameOrder is called when production columnNameOrder is entered.
func (s *BaseOdpsParserListener) EnterColumnNameOrder(ctx *ColumnNameOrderContext) {}

// ExitColumnNameOrder is called when production columnNameOrder is exited.
func (s *BaseOdpsParserListener) ExitColumnNameOrder(ctx *ColumnNameOrderContext) {}

// EnterColumnNameCommentList is called when production columnNameCommentList is entered.
func (s *BaseOdpsParserListener) EnterColumnNameCommentList(ctx *ColumnNameCommentListContext) {}

// ExitColumnNameCommentList is called when production columnNameCommentList is exited.
func (s *BaseOdpsParserListener) ExitColumnNameCommentList(ctx *ColumnNameCommentListContext) {}

// EnterColumnNameComment is called when production columnNameComment is entered.
func (s *BaseOdpsParserListener) EnterColumnNameComment(ctx *ColumnNameCommentContext) {}

// ExitColumnNameComment is called when production columnNameComment is exited.
func (s *BaseOdpsParserListener) ExitColumnNameComment(ctx *ColumnNameCommentContext) {}

// EnterColumnRefOrder is called when production columnRefOrder is entered.
func (s *BaseOdpsParserListener) EnterColumnRefOrder(ctx *ColumnRefOrderContext) {}

// ExitColumnRefOrder is called when production columnRefOrder is exited.
func (s *BaseOdpsParserListener) ExitColumnRefOrder(ctx *ColumnRefOrderContext) {}

// EnterColumnNameTypeConstraint is called when production columnNameTypeConstraint is entered.
func (s *BaseOdpsParserListener) EnterColumnNameTypeConstraint(ctx *ColumnNameTypeConstraintContext) {
}

// ExitColumnNameTypeConstraint is called when production columnNameTypeConstraint is exited.
func (s *BaseOdpsParserListener) ExitColumnNameTypeConstraint(ctx *ColumnNameTypeConstraintContext) {}

// EnterColumnNameType is called when production columnNameType is entered.
func (s *BaseOdpsParserListener) EnterColumnNameType(ctx *ColumnNameTypeContext) {}

// ExitColumnNameType is called when production columnNameType is exited.
func (s *BaseOdpsParserListener) ExitColumnNameType(ctx *ColumnNameTypeContext) {}

// EnterPartitionColumnNameType is called when production partitionColumnNameType is entered.
func (s *BaseOdpsParserListener) EnterPartitionColumnNameType(ctx *PartitionColumnNameTypeContext) {}

// ExitPartitionColumnNameType is called when production partitionColumnNameType is exited.
func (s *BaseOdpsParserListener) ExitPartitionColumnNameType(ctx *PartitionColumnNameTypeContext) {}

// EnterMultipartIdentifier is called when production multipartIdentifier is entered.
func (s *BaseOdpsParserListener) EnterMultipartIdentifier(ctx *MultipartIdentifierContext) {}

// ExitMultipartIdentifier is called when production multipartIdentifier is exited.
func (s *BaseOdpsParserListener) ExitMultipartIdentifier(ctx *MultipartIdentifierContext) {}

// EnterColumnNameTypeConstraintWithPos is called when production columnNameTypeConstraintWithPos is entered.
func (s *BaseOdpsParserListener) EnterColumnNameTypeConstraintWithPos(ctx *ColumnNameTypeConstraintWithPosContext) {
}

// ExitColumnNameTypeConstraintWithPos is called when production columnNameTypeConstraintWithPos is exited.
func (s *BaseOdpsParserListener) ExitColumnNameTypeConstraintWithPos(ctx *ColumnNameTypeConstraintWithPosContext) {
}

// EnterConstraints is called when production constraints is entered.
func (s *BaseOdpsParserListener) EnterConstraints(ctx *ConstraintsContext) {}

// ExitConstraints is called when production constraints is exited.
func (s *BaseOdpsParserListener) ExitConstraints(ctx *ConstraintsContext) {}

// EnterPrimaryKey is called when production primaryKey is entered.
func (s *BaseOdpsParserListener) EnterPrimaryKey(ctx *PrimaryKeyContext) {}

// ExitPrimaryKey is called when production primaryKey is exited.
func (s *BaseOdpsParserListener) ExitPrimaryKey(ctx *PrimaryKeyContext) {}

// EnterNullableSpec is called when production nullableSpec is entered.
func (s *BaseOdpsParserListener) EnterNullableSpec(ctx *NullableSpecContext) {}

// ExitNullableSpec is called when production nullableSpec is exited.
func (s *BaseOdpsParserListener) ExitNullableSpec(ctx *NullableSpecContext) {}

// EnterDefaultValue is called when production defaultValue is entered.
func (s *BaseOdpsParserListener) EnterDefaultValue(ctx *DefaultValueContext) {}

// ExitDefaultValue is called when production defaultValue is exited.
func (s *BaseOdpsParserListener) ExitDefaultValue(ctx *DefaultValueContext) {}

// EnterColumnNameColonType is called when production columnNameColonType is entered.
func (s *BaseOdpsParserListener) EnterColumnNameColonType(ctx *ColumnNameColonTypeContext) {}

// ExitColumnNameColonType is called when production columnNameColonType is exited.
func (s *BaseOdpsParserListener) ExitColumnNameColonType(ctx *ColumnNameColonTypeContext) {}

// EnterColType is called when production colType is entered.
func (s *BaseOdpsParserListener) EnterColType(ctx *ColTypeContext) {}

// ExitColType is called when production colType is exited.
func (s *BaseOdpsParserListener) ExitColType(ctx *ColTypeContext) {}

// EnterColTypeList is called when production colTypeList is entered.
func (s *BaseOdpsParserListener) EnterColTypeList(ctx *ColTypeListContext) {}

// ExitColTypeList is called when production colTypeList is exited.
func (s *BaseOdpsParserListener) ExitColTypeList(ctx *ColTypeListContext) {}

// EnterAnyType is called when production anyType is entered.
func (s *BaseOdpsParserListener) EnterAnyType(ctx *AnyTypeContext) {}

// ExitAnyType is called when production anyType is exited.
func (s *BaseOdpsParserListener) ExitAnyType(ctx *AnyTypeContext) {}

// EnterAnyTypeList is called when production anyTypeList is entered.
func (s *BaseOdpsParserListener) EnterAnyTypeList(ctx *AnyTypeListContext) {}

// ExitAnyTypeList is called when production anyTypeList is exited.
func (s *BaseOdpsParserListener) ExitAnyTypeList(ctx *AnyTypeListContext) {}

// EnterTableTypeInfo is called when production tableTypeInfo is entered.
func (s *BaseOdpsParserListener) EnterTableTypeInfo(ctx *TableTypeInfoContext) {}

// ExitTableTypeInfo is called when production tableTypeInfo is exited.
func (s *BaseOdpsParserListener) ExitTableTypeInfo(ctx *TableTypeInfoContext) {}

// EnterType is called when production type is entered.
func (s *BaseOdpsParserListener) EnterType(ctx *TypeContext) {}

// ExitType is called when production type is exited.
func (s *BaseOdpsParserListener) ExitType(ctx *TypeContext) {}

// EnterPrimitiveType is called when production primitiveType is entered.
func (s *BaseOdpsParserListener) EnterPrimitiveType(ctx *PrimitiveTypeContext) {}

// ExitPrimitiveType is called when production primitiveType is exited.
func (s *BaseOdpsParserListener) ExitPrimitiveType(ctx *PrimitiveTypeContext) {}

// EnterBuiltinTypeOrUdt is called when production builtinTypeOrUdt is entered.
func (s *BaseOdpsParserListener) EnterBuiltinTypeOrUdt(ctx *BuiltinTypeOrUdtContext) {}

// ExitBuiltinTypeOrUdt is called when production builtinTypeOrUdt is exited.
func (s *BaseOdpsParserListener) ExitBuiltinTypeOrUdt(ctx *BuiltinTypeOrUdtContext) {}

// EnterPrimitiveTypeOrUdt is called when production primitiveTypeOrUdt is entered.
func (s *BaseOdpsParserListener) EnterPrimitiveTypeOrUdt(ctx *PrimitiveTypeOrUdtContext) {}

// ExitPrimitiveTypeOrUdt is called when production primitiveTypeOrUdt is exited.
func (s *BaseOdpsParserListener) ExitPrimitiveTypeOrUdt(ctx *PrimitiveTypeOrUdtContext) {}

// EnterListType is called when production listType is entered.
func (s *BaseOdpsParserListener) EnterListType(ctx *ListTypeContext) {}

// ExitListType is called when production listType is exited.
func (s *BaseOdpsParserListener) ExitListType(ctx *ListTypeContext) {}

// EnterStructType is called when production structType is entered.
func (s *BaseOdpsParserListener) EnterStructType(ctx *StructTypeContext) {}

// ExitStructType is called when production structType is exited.
func (s *BaseOdpsParserListener) ExitStructType(ctx *StructTypeContext) {}

// EnterMapType is called when production mapType is entered.
func (s *BaseOdpsParserListener) EnterMapType(ctx *MapTypeContext) {}

// ExitMapType is called when production mapType is exited.
func (s *BaseOdpsParserListener) ExitMapType(ctx *MapTypeContext) {}

// EnterUnionType is called when production unionType is entered.
func (s *BaseOdpsParserListener) EnterUnionType(ctx *UnionTypeContext) {}

// ExitUnionType is called when production unionType is exited.
func (s *BaseOdpsParserListener) ExitUnionType(ctx *UnionTypeContext) {}

// EnterSetOperator is called when production setOperator is entered.
func (s *BaseOdpsParserListener) EnterSetOperator(ctx *SetOperatorContext) {}

// ExitSetOperator is called when production setOperator is exited.
func (s *BaseOdpsParserListener) ExitSetOperator(ctx *SetOperatorContext) {}

// EnterWithClause is called when production withClause is entered.
func (s *BaseOdpsParserListener) EnterWithClause(ctx *WithClauseContext) {}

// ExitWithClause is called when production withClause is exited.
func (s *BaseOdpsParserListener) ExitWithClause(ctx *WithClauseContext) {}

// EnterInsertClause is called when production insertClause is entered.
func (s *BaseOdpsParserListener) EnterInsertClause(ctx *InsertClauseContext) {}

// ExitInsertClause is called when production insertClause is exited.
func (s *BaseOdpsParserListener) ExitInsertClause(ctx *InsertClauseContext) {}

// EnterDestination is called when production destination is entered.
func (s *BaseOdpsParserListener) EnterDestination(ctx *DestinationContext) {}

// ExitDestination is called when production destination is exited.
func (s *BaseOdpsParserListener) ExitDestination(ctx *DestinationContext) {}

// EnterDeleteStatement is called when production deleteStatement is entered.
func (s *BaseOdpsParserListener) EnterDeleteStatement(ctx *DeleteStatementContext) {}

// ExitDeleteStatement is called when production deleteStatement is exited.
func (s *BaseOdpsParserListener) ExitDeleteStatement(ctx *DeleteStatementContext) {}

// EnterColumnAssignmentClause is called when production columnAssignmentClause is entered.
func (s *BaseOdpsParserListener) EnterColumnAssignmentClause(ctx *ColumnAssignmentClauseContext) {}

// ExitColumnAssignmentClause is called when production columnAssignmentClause is exited.
func (s *BaseOdpsParserListener) ExitColumnAssignmentClause(ctx *ColumnAssignmentClauseContext) {}

// EnterSetColumnsClause is called when production setColumnsClause is entered.
func (s *BaseOdpsParserListener) EnterSetColumnsClause(ctx *SetColumnsClauseContext) {}

// ExitSetColumnsClause is called when production setColumnsClause is exited.
func (s *BaseOdpsParserListener) ExitSetColumnsClause(ctx *SetColumnsClauseContext) {}

// EnterUpdateStatement is called when production updateStatement is entered.
func (s *BaseOdpsParserListener) EnterUpdateStatement(ctx *UpdateStatementContext) {}

// ExitUpdateStatement is called when production updateStatement is exited.
func (s *BaseOdpsParserListener) ExitUpdateStatement(ctx *UpdateStatementContext) {}

// EnterMergeStatement is called when production mergeStatement is entered.
func (s *BaseOdpsParserListener) EnterMergeStatement(ctx *MergeStatementContext) {}

// ExitMergeStatement is called when production mergeStatement is exited.
func (s *BaseOdpsParserListener) ExitMergeStatement(ctx *MergeStatementContext) {}

// EnterMergeTargetTable is called when production mergeTargetTable is entered.
func (s *BaseOdpsParserListener) EnterMergeTargetTable(ctx *MergeTargetTableContext) {}

// ExitMergeTargetTable is called when production mergeTargetTable is exited.
func (s *BaseOdpsParserListener) ExitMergeTargetTable(ctx *MergeTargetTableContext) {}

// EnterMergeSourceTable is called when production mergeSourceTable is entered.
func (s *BaseOdpsParserListener) EnterMergeSourceTable(ctx *MergeSourceTableContext) {}

// ExitMergeSourceTable is called when production mergeSourceTable is exited.
func (s *BaseOdpsParserListener) ExitMergeSourceTable(ctx *MergeSourceTableContext) {}

// EnterMergeAction is called when production mergeAction is entered.
func (s *BaseOdpsParserListener) EnterMergeAction(ctx *MergeActionContext) {}

// ExitMergeAction is called when production mergeAction is exited.
func (s *BaseOdpsParserListener) ExitMergeAction(ctx *MergeActionContext) {}

// EnterMergeValuesCaluse is called when production mergeValuesCaluse is entered.
func (s *BaseOdpsParserListener) EnterMergeValuesCaluse(ctx *MergeValuesCaluseContext) {}

// ExitMergeValuesCaluse is called when production mergeValuesCaluse is exited.
func (s *BaseOdpsParserListener) ExitMergeValuesCaluse(ctx *MergeValuesCaluseContext) {}

// EnterMergeSetColumnsClause is called when production mergeSetColumnsClause is entered.
func (s *BaseOdpsParserListener) EnterMergeSetColumnsClause(ctx *MergeSetColumnsClauseContext) {}

// ExitMergeSetColumnsClause is called when production mergeSetColumnsClause is exited.
func (s *BaseOdpsParserListener) ExitMergeSetColumnsClause(ctx *MergeSetColumnsClauseContext) {}

// EnterMergeColumnAssignmentClause is called when production mergeColumnAssignmentClause is entered.
func (s *BaseOdpsParserListener) EnterMergeColumnAssignmentClause(ctx *MergeColumnAssignmentClauseContext) {
}

// ExitMergeColumnAssignmentClause is called when production mergeColumnAssignmentClause is exited.
func (s *BaseOdpsParserListener) ExitMergeColumnAssignmentClause(ctx *MergeColumnAssignmentClauseContext) {
}

// EnterSelectClause is called when production selectClause is entered.
func (s *BaseOdpsParserListener) EnterSelectClause(ctx *SelectClauseContext) {}

// ExitSelectClause is called when production selectClause is exited.
func (s *BaseOdpsParserListener) ExitSelectClause(ctx *SelectClauseContext) {}

// EnterSelectList is called when production selectList is entered.
func (s *BaseOdpsParserListener) EnterSelectList(ctx *SelectListContext) {}

// ExitSelectList is called when production selectList is exited.
func (s *BaseOdpsParserListener) ExitSelectList(ctx *SelectListContext) {}

// EnterSelectTrfmClause is called when production selectTrfmClause is entered.
func (s *BaseOdpsParserListener) EnterSelectTrfmClause(ctx *SelectTrfmClauseContext) {}

// ExitSelectTrfmClause is called when production selectTrfmClause is exited.
func (s *BaseOdpsParserListener) ExitSelectTrfmClause(ctx *SelectTrfmClauseContext) {}

// EnterHintClause is called when production hintClause is entered.
func (s *BaseOdpsParserListener) EnterHintClause(ctx *HintClauseContext) {}

// ExitHintClause is called when production hintClause is exited.
func (s *BaseOdpsParserListener) ExitHintClause(ctx *HintClauseContext) {}

// EnterHintList is called when production hintList is entered.
func (s *BaseOdpsParserListener) EnterHintList(ctx *HintListContext) {}

// ExitHintList is called when production hintList is exited.
func (s *BaseOdpsParserListener) ExitHintList(ctx *HintListContext) {}

// EnterHintItem is called when production hintItem is entered.
func (s *BaseOdpsParserListener) EnterHintItem(ctx *HintItemContext) {}

// ExitHintItem is called when production hintItem is exited.
func (s *BaseOdpsParserListener) ExitHintItem(ctx *HintItemContext) {}

// EnterDynamicfilterHint is called when production dynamicfilterHint is entered.
func (s *BaseOdpsParserListener) EnterDynamicfilterHint(ctx *DynamicfilterHintContext) {}

// ExitDynamicfilterHint is called when production dynamicfilterHint is exited.
func (s *BaseOdpsParserListener) ExitDynamicfilterHint(ctx *DynamicfilterHintContext) {}

// EnterMapJoinHint is called when production mapJoinHint is entered.
func (s *BaseOdpsParserListener) EnterMapJoinHint(ctx *MapJoinHintContext) {}

// ExitMapJoinHint is called when production mapJoinHint is exited.
func (s *BaseOdpsParserListener) ExitMapJoinHint(ctx *MapJoinHintContext) {}

// EnterSkewJoinHint is called when production skewJoinHint is entered.
func (s *BaseOdpsParserListener) EnterSkewJoinHint(ctx *SkewJoinHintContext) {}

// ExitSkewJoinHint is called when production skewJoinHint is exited.
func (s *BaseOdpsParserListener) ExitSkewJoinHint(ctx *SkewJoinHintContext) {}

// EnterSelectivityHint is called when production selectivityHint is entered.
func (s *BaseOdpsParserListener) EnterSelectivityHint(ctx *SelectivityHintContext) {}

// ExitSelectivityHint is called when production selectivityHint is exited.
func (s *BaseOdpsParserListener) ExitSelectivityHint(ctx *SelectivityHintContext) {}

// EnterMultipleSkewHintArgs is called when production multipleSkewHintArgs is entered.
func (s *BaseOdpsParserListener) EnterMultipleSkewHintArgs(ctx *MultipleSkewHintArgsContext) {}

// ExitMultipleSkewHintArgs is called when production multipleSkewHintArgs is exited.
func (s *BaseOdpsParserListener) ExitMultipleSkewHintArgs(ctx *MultipleSkewHintArgsContext) {}

// EnterSkewJoinHintArgs is called when production skewJoinHintArgs is entered.
func (s *BaseOdpsParserListener) EnterSkewJoinHintArgs(ctx *SkewJoinHintArgsContext) {}

// ExitSkewJoinHintArgs is called when production skewJoinHintArgs is exited.
func (s *BaseOdpsParserListener) ExitSkewJoinHintArgs(ctx *SkewJoinHintArgsContext) {}

// EnterSkewColumns is called when production skewColumns is entered.
func (s *BaseOdpsParserListener) EnterSkewColumns(ctx *SkewColumnsContext) {}

// ExitSkewColumns is called when production skewColumns is exited.
func (s *BaseOdpsParserListener) ExitSkewColumns(ctx *SkewColumnsContext) {}

// EnterSkewJoinHintKeyValues is called when production skewJoinHintKeyValues is entered.
func (s *BaseOdpsParserListener) EnterSkewJoinHintKeyValues(ctx *SkewJoinHintKeyValuesContext) {}

// ExitSkewJoinHintKeyValues is called when production skewJoinHintKeyValues is exited.
func (s *BaseOdpsParserListener) ExitSkewJoinHintKeyValues(ctx *SkewJoinHintKeyValuesContext) {}

// EnterHintName is called when production hintName is entered.
func (s *BaseOdpsParserListener) EnterHintName(ctx *HintNameContext) {}

// ExitHintName is called when production hintName is exited.
func (s *BaseOdpsParserListener) ExitHintName(ctx *HintNameContext) {}

// EnterHintArgs is called when production hintArgs is entered.
func (s *BaseOdpsParserListener) EnterHintArgs(ctx *HintArgsContext) {}

// ExitHintArgs is called when production hintArgs is exited.
func (s *BaseOdpsParserListener) ExitHintArgs(ctx *HintArgsContext) {}

// EnterHintArgName is called when production hintArgName is entered.
func (s *BaseOdpsParserListener) EnterHintArgName(ctx *HintArgNameContext) {}

// ExitHintArgName is called when production hintArgName is exited.
func (s *BaseOdpsParserListener) ExitHintArgName(ctx *HintArgNameContext) {}

// EnterSelectItem is called when production selectItem is entered.
func (s *BaseOdpsParserListener) EnterSelectItem(ctx *SelectItemContext) {}

// ExitSelectItem is called when production selectItem is exited.
func (s *BaseOdpsParserListener) ExitSelectItem(ctx *SelectItemContext) {}

// EnterTrfmClause is called when production trfmClause is entered.
func (s *BaseOdpsParserListener) EnterTrfmClause(ctx *TrfmClauseContext) {}

// ExitTrfmClause is called when production trfmClause is exited.
func (s *BaseOdpsParserListener) ExitTrfmClause(ctx *TrfmClauseContext) {}

// EnterSelectExpression is called when production selectExpression is entered.
func (s *BaseOdpsParserListener) EnterSelectExpression(ctx *SelectExpressionContext) {}

// ExitSelectExpression is called when production selectExpression is exited.
func (s *BaseOdpsParserListener) ExitSelectExpression(ctx *SelectExpressionContext) {}

// EnterSelectExpressionList is called when production selectExpressionList is entered.
func (s *BaseOdpsParserListener) EnterSelectExpressionList(ctx *SelectExpressionListContext) {}

// ExitSelectExpressionList is called when production selectExpressionList is exited.
func (s *BaseOdpsParserListener) ExitSelectExpressionList(ctx *SelectExpressionListContext) {}

// EnterWindow_clause is called when production window_clause is entered.
func (s *BaseOdpsParserListener) EnterWindow_clause(ctx *Window_clauseContext) {}

// ExitWindow_clause is called when production window_clause is exited.
func (s *BaseOdpsParserListener) ExitWindow_clause(ctx *Window_clauseContext) {}

// EnterWindow_defn is called when production window_defn is entered.
func (s *BaseOdpsParserListener) EnterWindow_defn(ctx *Window_defnContext) {}

// ExitWindow_defn is called when production window_defn is exited.
func (s *BaseOdpsParserListener) ExitWindow_defn(ctx *Window_defnContext) {}

// EnterWindow_specification is called when production window_specification is entered.
func (s *BaseOdpsParserListener) EnterWindow_specification(ctx *Window_specificationContext) {}

// ExitWindow_specification is called when production window_specification is exited.
func (s *BaseOdpsParserListener) ExitWindow_specification(ctx *Window_specificationContext) {}

// EnterWindow_frame is called when production window_frame is entered.
func (s *BaseOdpsParserListener) EnterWindow_frame(ctx *Window_frameContext) {}

// ExitWindow_frame is called when production window_frame is exited.
func (s *BaseOdpsParserListener) ExitWindow_frame(ctx *Window_frameContext) {}

// EnterFrame_exclusion is called when production frame_exclusion is entered.
func (s *BaseOdpsParserListener) EnterFrame_exclusion(ctx *Frame_exclusionContext) {}

// ExitFrame_exclusion is called when production frame_exclusion is exited.
func (s *BaseOdpsParserListener) ExitFrame_exclusion(ctx *Frame_exclusionContext) {}

// EnterWindow_frame_start_boundary is called when production window_frame_start_boundary is entered.
func (s *BaseOdpsParserListener) EnterWindow_frame_start_boundary(ctx *Window_frame_start_boundaryContext) {
}

// ExitWindow_frame_start_boundary is called when production window_frame_start_boundary is exited.
func (s *BaseOdpsParserListener) ExitWindow_frame_start_boundary(ctx *Window_frame_start_boundaryContext) {
}

// EnterWindow_frame_boundary is called when production window_frame_boundary is entered.
func (s *BaseOdpsParserListener) EnterWindow_frame_boundary(ctx *Window_frame_boundaryContext) {}

// ExitWindow_frame_boundary is called when production window_frame_boundary is exited.
func (s *BaseOdpsParserListener) ExitWindow_frame_boundary(ctx *Window_frame_boundaryContext) {}

// EnterTableAllColumns is called when production tableAllColumns is entered.
func (s *BaseOdpsParserListener) EnterTableAllColumns(ctx *TableAllColumnsContext) {}

// ExitTableAllColumns is called when production tableAllColumns is exited.
func (s *BaseOdpsParserListener) ExitTableAllColumns(ctx *TableAllColumnsContext) {}

// EnterTableOrColumn is called when production tableOrColumn is entered.
func (s *BaseOdpsParserListener) EnterTableOrColumn(ctx *TableOrColumnContext) {}

// ExitTableOrColumn is called when production tableOrColumn is exited.
func (s *BaseOdpsParserListener) ExitTableOrColumn(ctx *TableOrColumnContext) {}

// EnterTableAndColumnRef is called when production tableAndColumnRef is entered.
func (s *BaseOdpsParserListener) EnterTableAndColumnRef(ctx *TableAndColumnRefContext) {}

// ExitTableAndColumnRef is called when production tableAndColumnRef is exited.
func (s *BaseOdpsParserListener) ExitTableAndColumnRef(ctx *TableAndColumnRefContext) {}

// EnterExpressionList is called when production expressionList is entered.
func (s *BaseOdpsParserListener) EnterExpressionList(ctx *ExpressionListContext) {}

// ExitExpressionList is called when production expressionList is exited.
func (s *BaseOdpsParserListener) ExitExpressionList(ctx *ExpressionListContext) {}

// EnterAliasList is called when production aliasList is entered.
func (s *BaseOdpsParserListener) EnterAliasList(ctx *AliasListContext) {}

// ExitAliasList is called when production aliasList is exited.
func (s *BaseOdpsParserListener) ExitAliasList(ctx *AliasListContext) {}

// EnterFromClause is called when production fromClause is entered.
func (s *BaseOdpsParserListener) EnterFromClause(ctx *FromClauseContext) {}

// ExitFromClause is called when production fromClause is exited.
func (s *BaseOdpsParserListener) ExitFromClause(ctx *FromClauseContext) {}

// EnterJoinSource is called when production joinSource is entered.
func (s *BaseOdpsParserListener) EnterJoinSource(ctx *JoinSourceContext) {}

// ExitJoinSource is called when production joinSource is exited.
func (s *BaseOdpsParserListener) ExitJoinSource(ctx *JoinSourceContext) {}

// EnterJoinRHS is called when production joinRHS is entered.
func (s *BaseOdpsParserListener) EnterJoinRHS(ctx *JoinRHSContext) {}

// ExitJoinRHS is called when production joinRHS is exited.
func (s *BaseOdpsParserListener) ExitJoinRHS(ctx *JoinRHSContext) {}

// EnterUniqueJoinSource is called when production uniqueJoinSource is entered.
func (s *BaseOdpsParserListener) EnterUniqueJoinSource(ctx *UniqueJoinSourceContext) {}

// ExitUniqueJoinSource is called when production uniqueJoinSource is exited.
func (s *BaseOdpsParserListener) ExitUniqueJoinSource(ctx *UniqueJoinSourceContext) {}

// EnterUniqueJoinExpr is called when production uniqueJoinExpr is entered.
func (s *BaseOdpsParserListener) EnterUniqueJoinExpr(ctx *UniqueJoinExprContext) {}

// ExitUniqueJoinExpr is called when production uniqueJoinExpr is exited.
func (s *BaseOdpsParserListener) ExitUniqueJoinExpr(ctx *UniqueJoinExprContext) {}

// EnterUniqueJoinToken is called when production uniqueJoinToken is entered.
func (s *BaseOdpsParserListener) EnterUniqueJoinToken(ctx *UniqueJoinTokenContext) {}

// ExitUniqueJoinToken is called when production uniqueJoinToken is exited.
func (s *BaseOdpsParserListener) ExitUniqueJoinToken(ctx *UniqueJoinTokenContext) {}

// EnterJoinToken is called when production joinToken is entered.
func (s *BaseOdpsParserListener) EnterJoinToken(ctx *JoinTokenContext) {}

// ExitJoinToken is called when production joinToken is exited.
func (s *BaseOdpsParserListener) ExitJoinToken(ctx *JoinTokenContext) {}

// EnterLateralView is called when production lateralView is entered.
func (s *BaseOdpsParserListener) EnterLateralView(ctx *LateralViewContext) {}

// ExitLateralView is called when production lateralView is exited.
func (s *BaseOdpsParserListener) ExitLateralView(ctx *LateralViewContext) {}

// EnterTableAlias is called when production tableAlias is entered.
func (s *BaseOdpsParserListener) EnterTableAlias(ctx *TableAliasContext) {}

// ExitTableAlias is called when production tableAlias is exited.
func (s *BaseOdpsParserListener) ExitTableAlias(ctx *TableAliasContext) {}

// EnterTableBucketSample is called when production tableBucketSample is entered.
func (s *BaseOdpsParserListener) EnterTableBucketSample(ctx *TableBucketSampleContext) {}

// ExitTableBucketSample is called when production tableBucketSample is exited.
func (s *BaseOdpsParserListener) ExitTableBucketSample(ctx *TableBucketSampleContext) {}

// EnterSplitSample is called when production splitSample is entered.
func (s *BaseOdpsParserListener) EnterSplitSample(ctx *SplitSampleContext) {}

// ExitSplitSample is called when production splitSample is exited.
func (s *BaseOdpsParserListener) ExitSplitSample(ctx *SplitSampleContext) {}

// EnterTableSample is called when production tableSample is entered.
func (s *BaseOdpsParserListener) EnterTableSample(ctx *TableSampleContext) {}

// ExitTableSample is called when production tableSample is exited.
func (s *BaseOdpsParserListener) ExitTableSample(ctx *TableSampleContext) {}

// EnterTableSource is called when production tableSource is entered.
func (s *BaseOdpsParserListener) EnterTableSource(ctx *TableSourceContext) {}

// ExitTableSource is called when production tableSource is exited.
func (s *BaseOdpsParserListener) ExitTableSource(ctx *TableSourceContext) {}

// EnterAvailableSql11KeywordsForOdpsTableAlias is called when production availableSql11KeywordsForOdpsTableAlias is entered.
func (s *BaseOdpsParserListener) EnterAvailableSql11KeywordsForOdpsTableAlias(ctx *AvailableSql11KeywordsForOdpsTableAliasContext) {
}

// ExitAvailableSql11KeywordsForOdpsTableAlias is called when production availableSql11KeywordsForOdpsTableAlias is exited.
func (s *BaseOdpsParserListener) ExitAvailableSql11KeywordsForOdpsTableAlias(ctx *AvailableSql11KeywordsForOdpsTableAliasContext) {
}

// EnterTableName is called when production tableName is entered.
func (s *BaseOdpsParserListener) EnterTableName(ctx *TableNameContext) {}

// ExitTableName is called when production tableName is exited.
func (s *BaseOdpsParserListener) ExitTableName(ctx *TableNameContext) {}

// EnterPartitioningSpec is called when production partitioningSpec is entered.
func (s *BaseOdpsParserListener) EnterPartitioningSpec(ctx *PartitioningSpecContext) {}

// ExitPartitioningSpec is called when production partitioningSpec is exited.
func (s *BaseOdpsParserListener) ExitPartitioningSpec(ctx *PartitioningSpecContext) {}

// EnterPartitionTableFunctionSource is called when production partitionTableFunctionSource is entered.
func (s *BaseOdpsParserListener) EnterPartitionTableFunctionSource(ctx *PartitionTableFunctionSourceContext) {
}

// ExitPartitionTableFunctionSource is called when production partitionTableFunctionSource is exited.
func (s *BaseOdpsParserListener) ExitPartitionTableFunctionSource(ctx *PartitionTableFunctionSourceContext) {
}

// EnterPartitionedTableFunction is called when production partitionedTableFunction is entered.
func (s *BaseOdpsParserListener) EnterPartitionedTableFunction(ctx *PartitionedTableFunctionContext) {
}

// ExitPartitionedTableFunction is called when production partitionedTableFunction is exited.
func (s *BaseOdpsParserListener) ExitPartitionedTableFunction(ctx *PartitionedTableFunctionContext) {}

// EnterWhereClause is called when production whereClause is entered.
func (s *BaseOdpsParserListener) EnterWhereClause(ctx *WhereClauseContext) {}

// ExitWhereClause is called when production whereClause is exited.
func (s *BaseOdpsParserListener) ExitWhereClause(ctx *WhereClauseContext) {}

// EnterValueRowConstructor is called when production valueRowConstructor is entered.
func (s *BaseOdpsParserListener) EnterValueRowConstructor(ctx *ValueRowConstructorContext) {}

// ExitValueRowConstructor is called when production valueRowConstructor is exited.
func (s *BaseOdpsParserListener) ExitValueRowConstructor(ctx *ValueRowConstructorContext) {}

// EnterValuesTableConstructor is called when production valuesTableConstructor is entered.
func (s *BaseOdpsParserListener) EnterValuesTableConstructor(ctx *ValuesTableConstructorContext) {}

// ExitValuesTableConstructor is called when production valuesTableConstructor is exited.
func (s *BaseOdpsParserListener) ExitValuesTableConstructor(ctx *ValuesTableConstructorContext) {}

// EnterValuesClause is called when production valuesClause is entered.
func (s *BaseOdpsParserListener) EnterValuesClause(ctx *ValuesClauseContext) {}

// ExitValuesClause is called when production valuesClause is exited.
func (s *BaseOdpsParserListener) ExitValuesClause(ctx *ValuesClauseContext) {}

// EnterVirtualTableSource is called when production virtualTableSource is entered.
func (s *BaseOdpsParserListener) EnterVirtualTableSource(ctx *VirtualTableSourceContext) {}

// ExitVirtualTableSource is called when production virtualTableSource is exited.
func (s *BaseOdpsParserListener) ExitVirtualTableSource(ctx *VirtualTableSourceContext) {}

// EnterTableNameColList is called when production tableNameColList is entered.
func (s *BaseOdpsParserListener) EnterTableNameColList(ctx *TableNameColListContext) {}

// ExitTableNameColList is called when production tableNameColList is exited.
func (s *BaseOdpsParserListener) ExitTableNameColList(ctx *TableNameColListContext) {}

// EnterFunctionTypeCubeOrRollup is called when production functionTypeCubeOrRollup is entered.
func (s *BaseOdpsParserListener) EnterFunctionTypeCubeOrRollup(ctx *FunctionTypeCubeOrRollupContext) {
}

// ExitFunctionTypeCubeOrRollup is called when production functionTypeCubeOrRollup is exited.
func (s *BaseOdpsParserListener) ExitFunctionTypeCubeOrRollup(ctx *FunctionTypeCubeOrRollupContext) {}

// EnterGroupingSetsItem is called when production groupingSetsItem is entered.
func (s *BaseOdpsParserListener) EnterGroupingSetsItem(ctx *GroupingSetsItemContext) {}

// ExitGroupingSetsItem is called when production groupingSetsItem is exited.
func (s *BaseOdpsParserListener) ExitGroupingSetsItem(ctx *GroupingSetsItemContext) {}

// EnterGroupingSetsClause is called when production groupingSetsClause is entered.
func (s *BaseOdpsParserListener) EnterGroupingSetsClause(ctx *GroupingSetsClauseContext) {}

// ExitGroupingSetsClause is called when production groupingSetsClause is exited.
func (s *BaseOdpsParserListener) ExitGroupingSetsClause(ctx *GroupingSetsClauseContext) {}

// EnterGroupByKey is called when production groupByKey is entered.
func (s *BaseOdpsParserListener) EnterGroupByKey(ctx *GroupByKeyContext) {}

// ExitGroupByKey is called when production groupByKey is exited.
func (s *BaseOdpsParserListener) ExitGroupByKey(ctx *GroupByKeyContext) {}

// EnterGroupByClause is called when production groupByClause is entered.
func (s *BaseOdpsParserListener) EnterGroupByClause(ctx *GroupByClauseContext) {}

// ExitGroupByClause is called when production groupByClause is exited.
func (s *BaseOdpsParserListener) ExitGroupByClause(ctx *GroupByClauseContext) {}

// EnterGroupingSetExpression is called when production groupingSetExpression is entered.
func (s *BaseOdpsParserListener) EnterGroupingSetExpression(ctx *GroupingSetExpressionContext) {}

// ExitGroupingSetExpression is called when production groupingSetExpression is exited.
func (s *BaseOdpsParserListener) ExitGroupingSetExpression(ctx *GroupingSetExpressionContext) {}

// EnterGroupingSetExpressionMultiple is called when production groupingSetExpressionMultiple is entered.
func (s *BaseOdpsParserListener) EnterGroupingSetExpressionMultiple(ctx *GroupingSetExpressionMultipleContext) {
}

// ExitGroupingSetExpressionMultiple is called when production groupingSetExpressionMultiple is exited.
func (s *BaseOdpsParserListener) ExitGroupingSetExpressionMultiple(ctx *GroupingSetExpressionMultipleContext) {
}

// EnterGroupingExpressionSingle is called when production groupingExpressionSingle is entered.
func (s *BaseOdpsParserListener) EnterGroupingExpressionSingle(ctx *GroupingExpressionSingleContext) {
}

// ExitGroupingExpressionSingle is called when production groupingExpressionSingle is exited.
func (s *BaseOdpsParserListener) ExitGroupingExpressionSingle(ctx *GroupingExpressionSingleContext) {}

// EnterHavingClause is called when production havingClause is entered.
func (s *BaseOdpsParserListener) EnterHavingClause(ctx *HavingClauseContext) {}

// ExitHavingClause is called when production havingClause is exited.
func (s *BaseOdpsParserListener) ExitHavingClause(ctx *HavingClauseContext) {}

// EnterHavingCondition is called when production havingCondition is entered.
func (s *BaseOdpsParserListener) EnterHavingCondition(ctx *HavingConditionContext) {}

// ExitHavingCondition is called when production havingCondition is exited.
func (s *BaseOdpsParserListener) ExitHavingCondition(ctx *HavingConditionContext) {}

// EnterExpressionsInParenthese is called when production expressionsInParenthese is entered.
func (s *BaseOdpsParserListener) EnterExpressionsInParenthese(ctx *ExpressionsInParentheseContext) {}

// ExitExpressionsInParenthese is called when production expressionsInParenthese is exited.
func (s *BaseOdpsParserListener) ExitExpressionsInParenthese(ctx *ExpressionsInParentheseContext) {}

// EnterExpressionsNotInParenthese is called when production expressionsNotInParenthese is entered.
func (s *BaseOdpsParserListener) EnterExpressionsNotInParenthese(ctx *ExpressionsNotInParentheseContext) {
}

// ExitExpressionsNotInParenthese is called when production expressionsNotInParenthese is exited.
func (s *BaseOdpsParserListener) ExitExpressionsNotInParenthese(ctx *ExpressionsNotInParentheseContext) {
}

// EnterColumnRefOrderInParenthese is called when production columnRefOrderInParenthese is entered.
func (s *BaseOdpsParserListener) EnterColumnRefOrderInParenthese(ctx *ColumnRefOrderInParentheseContext) {
}

// ExitColumnRefOrderInParenthese is called when production columnRefOrderInParenthese is exited.
func (s *BaseOdpsParserListener) ExitColumnRefOrderInParenthese(ctx *ColumnRefOrderInParentheseContext) {
}

// EnterColumnRefOrderNotInParenthese is called when production columnRefOrderNotInParenthese is entered.
func (s *BaseOdpsParserListener) EnterColumnRefOrderNotInParenthese(ctx *ColumnRefOrderNotInParentheseContext) {
}

// ExitColumnRefOrderNotInParenthese is called when production columnRefOrderNotInParenthese is exited.
func (s *BaseOdpsParserListener) ExitColumnRefOrderNotInParenthese(ctx *ColumnRefOrderNotInParentheseContext) {
}

// EnterOrderByClause is called when production orderByClause is entered.
func (s *BaseOdpsParserListener) EnterOrderByClause(ctx *OrderByClauseContext) {}

// ExitOrderByClause is called when production orderByClause is exited.
func (s *BaseOdpsParserListener) ExitOrderByClause(ctx *OrderByClauseContext) {}

// EnterColumnNameOrIndexInParenthese is called when production columnNameOrIndexInParenthese is entered.
func (s *BaseOdpsParserListener) EnterColumnNameOrIndexInParenthese(ctx *ColumnNameOrIndexInParentheseContext) {
}

// ExitColumnNameOrIndexInParenthese is called when production columnNameOrIndexInParenthese is exited.
func (s *BaseOdpsParserListener) ExitColumnNameOrIndexInParenthese(ctx *ColumnNameOrIndexInParentheseContext) {
}

// EnterColumnNameOrIndexNotInParenthese is called when production columnNameOrIndexNotInParenthese is entered.
func (s *BaseOdpsParserListener) EnterColumnNameOrIndexNotInParenthese(ctx *ColumnNameOrIndexNotInParentheseContext) {
}

// ExitColumnNameOrIndexNotInParenthese is called when production columnNameOrIndexNotInParenthese is exited.
func (s *BaseOdpsParserListener) ExitColumnNameOrIndexNotInParenthese(ctx *ColumnNameOrIndexNotInParentheseContext) {
}

// EnterColumnNameOrIndex is called when production columnNameOrIndex is entered.
func (s *BaseOdpsParserListener) EnterColumnNameOrIndex(ctx *ColumnNameOrIndexContext) {}

// ExitColumnNameOrIndex is called when production columnNameOrIndex is exited.
func (s *BaseOdpsParserListener) ExitColumnNameOrIndex(ctx *ColumnNameOrIndexContext) {}

// EnterZorderByClause is called when production zorderByClause is entered.
func (s *BaseOdpsParserListener) EnterZorderByClause(ctx *ZorderByClauseContext) {}

// ExitZorderByClause is called when production zorderByClause is exited.
func (s *BaseOdpsParserListener) ExitZorderByClause(ctx *ZorderByClauseContext) {}

// EnterClusterByClause is called when production clusterByClause is entered.
func (s *BaseOdpsParserListener) EnterClusterByClause(ctx *ClusterByClauseContext) {}

// ExitClusterByClause is called when production clusterByClause is exited.
func (s *BaseOdpsParserListener) ExitClusterByClause(ctx *ClusterByClauseContext) {}

// EnterPartitionByClause is called when production partitionByClause is entered.
func (s *BaseOdpsParserListener) EnterPartitionByClause(ctx *PartitionByClauseContext) {}

// ExitPartitionByClause is called when production partitionByClause is exited.
func (s *BaseOdpsParserListener) ExitPartitionByClause(ctx *PartitionByClauseContext) {}

// EnterDistributeByClause is called when production distributeByClause is entered.
func (s *BaseOdpsParserListener) EnterDistributeByClause(ctx *DistributeByClauseContext) {}

// ExitDistributeByClause is called when production distributeByClause is exited.
func (s *BaseOdpsParserListener) ExitDistributeByClause(ctx *DistributeByClauseContext) {}

// EnterSortByClause is called when production sortByClause is entered.
func (s *BaseOdpsParserListener) EnterSortByClause(ctx *SortByClauseContext) {}

// ExitSortByClause is called when production sortByClause is exited.
func (s *BaseOdpsParserListener) ExitSortByClause(ctx *SortByClauseContext) {}

// EnterFunction is called when production function is entered.
func (s *BaseOdpsParserListener) EnterFunction(ctx *FunctionContext) {}

// ExitFunction is called when production function is exited.
func (s *BaseOdpsParserListener) ExitFunction(ctx *FunctionContext) {}

// EnterFunctionArgument is called when production functionArgument is entered.
func (s *BaseOdpsParserListener) EnterFunctionArgument(ctx *FunctionArgumentContext) {}

// ExitFunctionArgument is called when production functionArgument is exited.
func (s *BaseOdpsParserListener) ExitFunctionArgument(ctx *FunctionArgumentContext) {}

// EnterBuiltinFunctionStructure is called when production builtinFunctionStructure is entered.
func (s *BaseOdpsParserListener) EnterBuiltinFunctionStructure(ctx *BuiltinFunctionStructureContext) {
}

// ExitBuiltinFunctionStructure is called when production builtinFunctionStructure is exited.
func (s *BaseOdpsParserListener) ExitBuiltinFunctionStructure(ctx *BuiltinFunctionStructureContext) {}

// EnterFunctionName is called when production functionName is entered.
func (s *BaseOdpsParserListener) EnterFunctionName(ctx *FunctionNameContext) {}

// ExitFunctionName is called when production functionName is exited.
func (s *BaseOdpsParserListener) ExitFunctionName(ctx *FunctionNameContext) {}

// EnterCastExpression is called when production castExpression is entered.
func (s *BaseOdpsParserListener) EnterCastExpression(ctx *CastExpressionContext) {}

// ExitCastExpression is called when production castExpression is exited.
func (s *BaseOdpsParserListener) ExitCastExpression(ctx *CastExpressionContext) {}

// EnterCaseExpression is called when production caseExpression is entered.
func (s *BaseOdpsParserListener) EnterCaseExpression(ctx *CaseExpressionContext) {}

// ExitCaseExpression is called when production caseExpression is exited.
func (s *BaseOdpsParserListener) ExitCaseExpression(ctx *CaseExpressionContext) {}

// EnterWhenExpression is called when production whenExpression is entered.
func (s *BaseOdpsParserListener) EnterWhenExpression(ctx *WhenExpressionContext) {}

// ExitWhenExpression is called when production whenExpression is exited.
func (s *BaseOdpsParserListener) ExitWhenExpression(ctx *WhenExpressionContext) {}

// EnterConstant is called when production constant is entered.
func (s *BaseOdpsParserListener) EnterConstant(ctx *ConstantContext) {}

// ExitConstant is called when production constant is exited.
func (s *BaseOdpsParserListener) ExitConstant(ctx *ConstantContext) {}

// EnterSimpleStringLiteral is called when production simpleStringLiteral is entered.
func (s *BaseOdpsParserListener) EnterSimpleStringLiteral(ctx *SimpleStringLiteralContext) {}

// ExitSimpleStringLiteral is called when production simpleStringLiteral is exited.
func (s *BaseOdpsParserListener) ExitSimpleStringLiteral(ctx *SimpleStringLiteralContext) {}

// EnterStringLiteral is called when production stringLiteral is entered.
func (s *BaseOdpsParserListener) EnterStringLiteral(ctx *StringLiteralContext) {}

// ExitStringLiteral is called when production stringLiteral is exited.
func (s *BaseOdpsParserListener) ExitStringLiteral(ctx *StringLiteralContext) {}

// EnterDoubleQuoteStringLiteral is called when production doubleQuoteStringLiteral is entered.
func (s *BaseOdpsParserListener) EnterDoubleQuoteStringLiteral(ctx *DoubleQuoteStringLiteralContext) {
}

// ExitDoubleQuoteStringLiteral is called when production doubleQuoteStringLiteral is exited.
func (s *BaseOdpsParserListener) ExitDoubleQuoteStringLiteral(ctx *DoubleQuoteStringLiteralContext) {}

// EnterCharSetStringLiteral is called when production charSetStringLiteral is entered.
func (s *BaseOdpsParserListener) EnterCharSetStringLiteral(ctx *CharSetStringLiteralContext) {}

// ExitCharSetStringLiteral is called when production charSetStringLiteral is exited.
func (s *BaseOdpsParserListener) ExitCharSetStringLiteral(ctx *CharSetStringLiteralContext) {}

// EnterDateLiteral is called when production dateLiteral is entered.
func (s *BaseOdpsParserListener) EnterDateLiteral(ctx *DateLiteralContext) {}

// ExitDateLiteral is called when production dateLiteral is exited.
func (s *BaseOdpsParserListener) ExitDateLiteral(ctx *DateLiteralContext) {}

// EnterDateTimeLiteral is called when production dateTimeLiteral is entered.
func (s *BaseOdpsParserListener) EnterDateTimeLiteral(ctx *DateTimeLiteralContext) {}

// ExitDateTimeLiteral is called when production dateTimeLiteral is exited.
func (s *BaseOdpsParserListener) ExitDateTimeLiteral(ctx *DateTimeLiteralContext) {}

// EnterTimestampLiteral is called when production timestampLiteral is entered.
func (s *BaseOdpsParserListener) EnterTimestampLiteral(ctx *TimestampLiteralContext) {}

// ExitTimestampLiteral is called when production timestampLiteral is exited.
func (s *BaseOdpsParserListener) ExitTimestampLiteral(ctx *TimestampLiteralContext) {}

// EnterIntervalLiteral is called when production intervalLiteral is entered.
func (s *BaseOdpsParserListener) EnterIntervalLiteral(ctx *IntervalLiteralContext) {}

// ExitIntervalLiteral is called when production intervalLiteral is exited.
func (s *BaseOdpsParserListener) ExitIntervalLiteral(ctx *IntervalLiteralContext) {}

// EnterIntervalQualifiers is called when production intervalQualifiers is entered.
func (s *BaseOdpsParserListener) EnterIntervalQualifiers(ctx *IntervalQualifiersContext) {}

// ExitIntervalQualifiers is called when production intervalQualifiers is exited.
func (s *BaseOdpsParserListener) ExitIntervalQualifiers(ctx *IntervalQualifiersContext) {}

// EnterIntervalQualifiersUnit is called when production intervalQualifiersUnit is entered.
func (s *BaseOdpsParserListener) EnterIntervalQualifiersUnit(ctx *IntervalQualifiersUnitContext) {}

// ExitIntervalQualifiersUnit is called when production intervalQualifiersUnit is exited.
func (s *BaseOdpsParserListener) ExitIntervalQualifiersUnit(ctx *IntervalQualifiersUnitContext) {}

// EnterIntervalQualifierPrecision is called when production intervalQualifierPrecision is entered.
func (s *BaseOdpsParserListener) EnterIntervalQualifierPrecision(ctx *IntervalQualifierPrecisionContext) {
}

// ExitIntervalQualifierPrecision is called when production intervalQualifierPrecision is exited.
func (s *BaseOdpsParserListener) ExitIntervalQualifierPrecision(ctx *IntervalQualifierPrecisionContext) {
}

// EnterBooleanValue is called when production booleanValue is entered.
func (s *BaseOdpsParserListener) EnterBooleanValue(ctx *BooleanValueContext) {}

// ExitBooleanValue is called when production booleanValue is exited.
func (s *BaseOdpsParserListener) ExitBooleanValue(ctx *BooleanValueContext) {}

// EnterTableOrPartition is called when production tableOrPartition is entered.
func (s *BaseOdpsParserListener) EnterTableOrPartition(ctx *TableOrPartitionContext) {}

// ExitTableOrPartition is called when production tableOrPartition is exited.
func (s *BaseOdpsParserListener) ExitTableOrPartition(ctx *TableOrPartitionContext) {}

// EnterPartitionSpec is called when production partitionSpec is entered.
func (s *BaseOdpsParserListener) EnterPartitionSpec(ctx *PartitionSpecContext) {}

// ExitPartitionSpec is called when production partitionSpec is exited.
func (s *BaseOdpsParserListener) ExitPartitionSpec(ctx *PartitionSpecContext) {}

// EnterPartitionVal is called when production partitionVal is entered.
func (s *BaseOdpsParserListener) EnterPartitionVal(ctx *PartitionValContext) {}

// ExitPartitionVal is called when production partitionVal is exited.
func (s *BaseOdpsParserListener) ExitPartitionVal(ctx *PartitionValContext) {}

// EnterDateWithoutQuote is called when production dateWithoutQuote is entered.
func (s *BaseOdpsParserListener) EnterDateWithoutQuote(ctx *DateWithoutQuoteContext) {}

// ExitDateWithoutQuote is called when production dateWithoutQuote is exited.
func (s *BaseOdpsParserListener) ExitDateWithoutQuote(ctx *DateWithoutQuoteContext) {}

// EnterDropPartitionSpec is called when production dropPartitionSpec is entered.
func (s *BaseOdpsParserListener) EnterDropPartitionSpec(ctx *DropPartitionSpecContext) {}

// ExitDropPartitionSpec is called when production dropPartitionSpec is exited.
func (s *BaseOdpsParserListener) ExitDropPartitionSpec(ctx *DropPartitionSpecContext) {}

// EnterSysFuncNames is called when production sysFuncNames is entered.
func (s *BaseOdpsParserListener) EnterSysFuncNames(ctx *SysFuncNamesContext) {}

// ExitSysFuncNames is called when production sysFuncNames is exited.
func (s *BaseOdpsParserListener) ExitSysFuncNames(ctx *SysFuncNamesContext) {}

// EnterDescFuncNames is called when production descFuncNames is entered.
func (s *BaseOdpsParserListener) EnterDescFuncNames(ctx *DescFuncNamesContext) {}

// ExitDescFuncNames is called when production descFuncNames is exited.
func (s *BaseOdpsParserListener) ExitDescFuncNames(ctx *DescFuncNamesContext) {}

// EnterFunctionIdentifier is called when production functionIdentifier is entered.
func (s *BaseOdpsParserListener) EnterFunctionIdentifier(ctx *FunctionIdentifierContext) {}

// ExitFunctionIdentifier is called when production functionIdentifier is exited.
func (s *BaseOdpsParserListener) ExitFunctionIdentifier(ctx *FunctionIdentifierContext) {}

// EnterReserved is called when production reserved is entered.
func (s *BaseOdpsParserListener) EnterReserved(ctx *ReservedContext) {}

// ExitReserved is called when production reserved is exited.
func (s *BaseOdpsParserListener) ExitReserved(ctx *ReservedContext) {}

// EnterNonReserved is called when production nonReserved is entered.
func (s *BaseOdpsParserListener) EnterNonReserved(ctx *NonReservedContext) {}

// ExitNonReserved is called when production nonReserved is exited.
func (s *BaseOdpsParserListener) ExitNonReserved(ctx *NonReservedContext) {}

// EnterSql11ReservedKeywordsUsedAsCastFunctionName is called when production sql11ReservedKeywordsUsedAsCastFunctionName is entered.
func (s *BaseOdpsParserListener) EnterSql11ReservedKeywordsUsedAsCastFunctionName(ctx *Sql11ReservedKeywordsUsedAsCastFunctionNameContext) {
}

// ExitSql11ReservedKeywordsUsedAsCastFunctionName is called when production sql11ReservedKeywordsUsedAsCastFunctionName is exited.
func (s *BaseOdpsParserListener) ExitSql11ReservedKeywordsUsedAsCastFunctionName(ctx *Sql11ReservedKeywordsUsedAsCastFunctionNameContext) {
}

// EnterSql11ReservedKeywordsUsedAsIdentifier is called when production sql11ReservedKeywordsUsedAsIdentifier is entered.
func (s *BaseOdpsParserListener) EnterSql11ReservedKeywordsUsedAsIdentifier(ctx *Sql11ReservedKeywordsUsedAsIdentifierContext) {
}

// ExitSql11ReservedKeywordsUsedAsIdentifier is called when production sql11ReservedKeywordsUsedAsIdentifier is exited.
func (s *BaseOdpsParserListener) ExitSql11ReservedKeywordsUsedAsIdentifier(ctx *Sql11ReservedKeywordsUsedAsIdentifierContext) {
}
