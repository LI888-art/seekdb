/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_explain_resolver.h"
#include "sql/resolver/ob_resolver.h"
#include <cstring>
#include <strings.h>
#include "sql/resolver/dml/ob_select_resolver.h"

using namespace oceanbase;
using namespace sql;
using namespace common;
const static int64_t EXPLAIN_FORMAT = 0;
const static int64_t EXPLAIN_DISPLAY_OPTION = 1;
const static int64_t EXPLAIN_INTO_TABLE = 2;
const static int64_t EXPLAIN_STMT_ID = 3;
const static int64_t EXPLAIN_CHILD_STMT = 4;

namespace {

static bool file_sql_is_read_func(const ParseNode *node)
{
  if (OB_ISNULL(node) || OB_ISNULL(node->str_value_) || 0 == node->str_len_) {
    return false;
  }
  ObString name(node->str_len_, node->str_value_);
  return 0 == name.case_compare("read_csv")
      || 0 == name.case_compare("read_json")
      || 0 == name.case_compare("read_ndjson")
      || 0 == name.case_compare("read_parquet");
}

static bool file_sql_contains_read_expr(const ParseNode *node)
{
  if (OB_ISNULL(node)) return false;
  if (T_JSON_TABLE_EXPRESSION == node->type_ && node->num_child_ >= 1) {
    if (file_sql_is_read_func(node->children_[0])) return true;
  }
  for (int64_t i = 0; i < node->num_child_; ++i) {
    if (file_sql_contains_read_expr(node->children_[i])) return true;
  }
  return false;
}

static const char *file_sql_normalize_type_name(ObObjType type)
{
  const char *raw = ob_obj_type_str(type);
  if (OB_ISNULL(raw)) return "VARCHAR";
  if (0 == strncasecmp(raw, "MYSQL_", 6)) {
    raw += 6;
  }
  return raw;
}

static int file_sql_build_describe_meta_stmt(
    ObResolverParams &params,
    ObSelectStmt &child_stmt,
    ObSelectStmt *&meta_stmt)
{
  int ret = OB_SUCCESS;
  meta_stmt = nullptr;

  ObIArray<TableItem*> &items = child_stmt.get_table_items();
  TableItem *file_item = nullptr;
  for (int64_t i = 0; OB_ISNULL(file_item) && i < items.count(); ++i) {
    TableItem *it = items.at(i);
    if (OB_NOT_NULL(it) && TableItem::GENERATED_TABLE == it->type_
        && OB_NOT_NULL(it->ref_query_)) {
      file_item = it;
    }
  }
  if (OB_ISNULL(file_item)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObIArray<SelectItem> &sel_items = file_item->ref_query_->get_select_items();
    if (sel_items.empty()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObSqlString sql;
      for (int64_t i = 0; OB_SUCC(ret) && i < sel_items.count(); ++i) {
        const SelectItem &si = sel_items.at(i);
        ObString col_name = si.alias_name_;
        if (col_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          const char *type_str = "VARCHAR";
          if (OB_NOT_NULL(si.expr_)) {
            type_str = file_sql_normalize_type_name(si.expr_->get_result_type().get_type());
          }
          if (i > 0 && OB_FAIL(sql.append(" UNION ALL "))) {
          } else if (OB_FAIL(sql.append_fmt(
              "SELECT '%.*s' AS column_name, '%s' AS column_type, "
              "'YES' AS `null`, NULL AS `key`, NULL AS `default`, NULL AS `extra`",
              col_name.length(), col_name.ptr(), type_str))) {
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObParser parser(*params.allocator_, params.session_info_->get_sql_mode(),
                        params.session_info_->get_charsets4parser());
        ParseResult pr;
        if (OB_FAIL(parser.parse(sql.string(), pr))) {
          LOG_WARN("parse describe meta failed", K(ret), K(sql));
        } else if (OB_ISNULL(pr.result_tree_)
                   || OB_ISNULL(pr.result_tree_->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          ObSelectResolver sel_resolver(params);
          if (OB_FAIL(sel_resolver.resolve(*pr.result_tree_->children_[0]))) {
            LOG_WARN("resolve describe meta failed", K(ret));
          } else if (OB_ISNULL(meta_stmt = sel_resolver.get_select_stmt())) {
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
    }
  }
  return ret;
}

}  // anonymous namespace

int ObExplainResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;

  // ===== FILE_SQL_DESCRIBE_INTERCEPT_BEGIN =====
  if (T_EXPLAIN == parse_tree.type_ && 5 == parse_tree.num_child_) {
    const ParseNode *child_node = parse_tree.children_[EXPLAIN_CHILD_STMT];
    if (OB_NOT_NULL(child_node) && T_SELECT == child_node->type_
        && file_sql_contains_read_expr(child_node)) {
      ObStmt *child_stmt = nullptr;
      ObResolver resolver(params_);
      if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *child_node, child_stmt))) {
        LOG_WARN("failed to resolve inner select for describe", K(ret));
      } else if (OB_ISNULL(child_stmt) || !child_stmt->is_select_stmt()) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObSelectStmt *meta_stmt = nullptr;
        if (OB_FAIL(file_sql_build_describe_meta_stmt(params_,
                *static_cast<ObSelectStmt*>(child_stmt), meta_stmt))) {
          LOG_WARN("failed to build describe meta stmt", K(ret));
        } else if (OB_ISNULL(meta_stmt)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          stmt_ = meta_stmt;
          return OB_SUCCESS;
        }
      }
    }
  }
  // ===== FILE_SQL_DESCRIBE_INTERCEPT_END =====


  ObExplainStmt *explain_stmt = NULL;
  if (!(T_EXPLAIN == parse_tree.type_  && 5 == parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid EXPLAIN syntax",
             "type", parse_tree.type_,
             "# of child", parse_tree.num_child_);
  } else if (NULL == (explain_stmt = create_stmt<ObExplainStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create explain stmt");
  } else {
    ParseNode *child_node = parse_tree.children_[EXPLAIN_CHILD_STMT];
    ObStmt *child_stmt = NULL;
    ObExplainDisplayOpt opt;
    opt.with_tree_line_ = true;
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty parse tree for stmt");
    } else if (!(T_SELECT == child_node->type_
                 || T_DELETE == child_node->type_
                 || T_INSERT == child_node->type_
                 || T_UPDATE == child_node->type_
                 || T_REPLACE == child_node->type_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid stmt type for EXPLAIN", "type", child_node->type_);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "stmt type for EXPLAIN");
    } else {
      ObResolver resolver(params_);
      if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                   *child_node,
                                   child_stmt))) {
      } else if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate stmt for child stmt");
      } else {
        explain_stmt->set_explain_query_stmt(static_cast<ObDMLStmt *>(child_stmt));
        ParseNode *format = parse_tree.children_[EXPLAIN_FORMAT];
        if (NULL != format) {
          switch (format->type_) {
          case T_OUTLINE:
          {
            explain_stmt->set_explain_format(EXPLAIN_OUTLINE);
            opt.with_tree_line_ = true;
          } break;
          case T_EXTENDED:
          {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED);
            opt.with_tree_line_ = true;
          } break;
          case T_PARTITIONS:
          {
            explain_stmt->set_explain_format(EXPLAIN_PARTITIONS);
            opt.with_tree_line_ = true;
          } break;
          case T_TRADITIONAL:
          {
            explain_stmt->set_explain_format(EXPLAIN_TRADITIONAL);
            opt.with_tree_line_ = true;
          } break;
          case T_FORMAT_JSON:
          {
            explain_stmt->set_explain_format(EXPLAIN_FORMAT_JSON);
            opt.with_tree_line_ = false;
          } break;
          case T_BASIC: {
            explain_stmt->set_explain_format(EXPLAIN_BASIC);
            opt.with_tree_line_ = true;
            break;
          }
          case T_EXTENDED_NOADDR: {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED_NOADDR);
            opt.with_tree_line_ = true;
            break;
          }
          case T_PLANREGRESS: {
            explain_stmt->set_explain_format(EXPLAIN_PLANREGRESS);
            opt.with_tree_line_ = true;
            break;
          }
          default:
            ret = OB_ERROR;
            LOG_WARN("unknown explain format", "format", format->type_);
            break;
          }
        }
      }
    }
    ParseNode *opt_node = parse_tree.children_[EXPLAIN_DISPLAY_OPTION];
    if (OB_SUCC(ret) && NULL != opt_node) {
      switch (opt_node->type_) {
      case T_PRETTY_COLOR:
        opt.with_color_ = true;
        // pass
      case T_PRETTY:
        opt.with_tree_line_ = true;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      explain_stmt->set_display_opt(opt);
    }
    ParseNode *into_table_node = parse_tree.children_[EXPLAIN_INTO_TABLE];
    if (OB_SUCC(ret) && NULL != into_table_node) {
      ObString into_table(into_table_node->str_len_, into_table_node->str_value_);
      explain_stmt->set_into_table(into_table);
    }
    ParseNode *statement_id_node = parse_tree.children_[EXPLAIN_STMT_ID];
    if (OB_SUCC(ret) && NULL != statement_id_node) {
      ObString statement_id(statement_id_node->str_len_, statement_id_node->str_value_);
      explain_stmt->set_statement_id(statement_id);
    }
  }

  return ret;
}
