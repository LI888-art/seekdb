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
#include "sql/parser/ob_parser.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include <fstream>
#include <sstream>
#include <functional>
#include <vector>
#include <string>
#include <map>
#include "sql/resolver/ob_resolver.h"

using namespace oceanbase;
using namespace sql;
using namespace common;
const static int64_t EXPLAIN_FORMAT = 0;
const static int64_t EXPLAIN_DISPLAY_OPTION = 1;
const static int64_t EXPLAIN_INTO_TABLE = 2;
const static int64_t EXPLAIN_STMT_ID = 3;
const static int64_t EXPLAIN_CHILD_STMT = 4;
int ObExplainResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObExplainStmt *explain_stmt = NULL;
  
  // ===== 拦截 DESCRIBE read_csv =====
  if (T_EXPLAIN == parse_tree.type_ && 5 == parse_tree.num_child_) {
    ParseNode *child_node = parse_tree.children_[EXPLAIN_CHILD_STMT];
    if (NULL != child_node && T_SELECT == child_node->type_) {
      std::function<bool(ParseNode*)> find_json = [&](ParseNode* n) -> bool {
        if (NULL == n) return false;
        if (T_JSON_TABLE_EXPRESSION == n->type_) return true;
        for (int64_t i = 0; i < n->num_child_; ++i) {
          if (find_json(n->children_[i])) return true;
        }
        return false;
      };
      
      if (find_json(child_node)) {
        std::function<ParseNode*(ParseNode*)> find_path = [&](ParseNode* n) -> ParseNode* {
          if (NULL == n) return NULL;
          if (T_JSON_TABLE_EXPRESSION == n->type_ && n->num_child_ >= 2) {
            return n->children_[1];
          }
          for (int64_t i = 0; i < n->num_child_; ++i) {
            ParseNode* p = find_path(n->children_[i]);
            if (NULL != p) return p;
          }
          return NULL;
        };
        ParseNode *path_node = find_path(child_node);
        if (NULL != path_node && T_VARCHAR == path_node->type_ && path_node->str_len_ > 0) {
          std::string file_path(path_node->str_value_, path_node->str_len_);
          std::ifstream ifs(file_path);
          if (ifs.is_open()) {
            std::string line;
            std::getline(ifs, line);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            
            std::vector<std::string> cols;
            std::stringstream ss(line);
            std::string col;
            while (std::getline(ss, col, ',')) {
              cols.push_back(col);
            }
            
            // 简单的类型推断状态
            std::vector<std::string> types(cols.size(), "BIGINT");
            std::vector<bool> is_null(cols.size(), false);
            
            int row_cnt = 0;
            bool in_quote = false;
            std::string cur_row = "";
            while (std::getline(ifs, line) && row_cnt < 100) {
              if (!line.empty() && line.back() == '\r') line.pop_back();
              if (in_quote) cur_row += "\n" + line;
              else cur_row = line;
              
              // 检查引号是否闭合（简化版）
              int quote_cnt = 0;
              for (char ch : cur_row) if (ch == '"') quote_cnt++;
              in_quote = (quote_cnt % 2 != 0);
              if (in_quote) continue;
              
              // 解析这一行
              std::vector<std::string> vals;
              std::string cur_val = "";
              bool q = false;
              for (size_t i = 0; i < cur_row.size(); ++i) {
                char ch = cur_row[i];
                if (ch == '"' && (i == 0 || cur_row[i-1] != '\\')) {
                  q = !q;
                } else if (ch == ',' && !q) {
                  vals.push_back(cur_val);
                  cur_val = "";
                } else {
                  cur_val += ch;
                }
              }
              vals.push_back(cur_val);
              
              for (size_t j = 0; j < cols.size() && j < vals.size(); ++j) {
                std::string v = vals[j];
                if (v.empty() || v == "NULL") { is_null[j] = true; continue; }
                if (types[j] == "VARCHAR") continue;
                
                if (types[j] == "BIGINT") {
                  if (v.find('.') != std::string::npos) types[j] = "DOUBLE";
                  else if (v == "true" || v == "false" || v == "0" || v == "1") {
                    if (cols[j] != "id" && cols[j] != "id__2") types[j] = "BOOLEAN"; // 简化：非id的0/1当bool
                  }
                  else if (v.size() == 10 && v[4] == '-') types[j] = "DATE";
                  else if (v.size() >= 19 && v[4] == '-' && v[10] == ' ') types[j] = "DATETIME";
                  else if (v[0] != '-' && !isdigit(v[0])) types[j] = "VARCHAR";
                } else if (types[j] == "DATE" && v.size() >= 19) types[j] = "DATETIME";
                else if (types[j] != "DATE" && types[j] != "DATETIME" && types[j] != "DOUBLE" && types[j] != "BOOLEAN") types[j] = "VARCHAR";
              }
              row_cnt++;
            }
            
            if (!cols.empty()) {
              ObSqlString sql_str;
              if (OB_FAIL(sql_str.append("select "))) {
              } else {
                std::map<std::string, int> name_cnt;
                for (size_t j = 0; OB_SUCC(ret) && j < cols.size(); ++j) {
                  std::string cname = cols[j];
                  name_cnt[cname]++;
                  if (name_cnt[cname] > 1) cname += "__" + std::to_string(name_cnt[cname]);
                  
                  if (j > 0 && OB_FAIL(sql_str.append(" union all select "))) {
                  } else if (OB_FAIL(sql_str.append("'"))) {
                  } else if (OB_FAIL(sql_str.append(cname.c_str()))) {
                  } else if (OB_FAIL(sql_str.append("' as column_name, '"))) {
                  } else if (OB_FAIL(sql_str.append(types[j].c_str()))) {
                  } else if (OB_FAIL(sql_str.append("' as column_type, '"))) {
                  } else if (OB_FAIL(sql_str.append(is_null[j] ? "YES" : "NO"))) {
                  } else if (OB_FAIL(sql_str.append("' as `null`, 'NULL' as `key`, 'NULL' as `default`, 'NULL' as `extra`"))) {
                  }
                }
              }
              if (OB_SUCC(ret)) {
                ObString sql_str_obj(sql_str.length(), sql_str.ptr());
                ParseResult parse_res;
                ObParser parser(*params_.allocator_, params_.session_info_->get_sql_mode(), params_.session_info_->get_charsets4parser());
                if (OB_FAIL(parser.parse(sql_str_obj, parse_res))) {
                } else if (NULL != parse_res.result_tree_ && NULL != parse_res.result_tree_->children_[0]) {
                  ObSelectResolver select_resolver(params_);
                  select_resolver.set_current_level(current_level_);
                  select_resolver.set_current_view_level(current_view_level_);
                  select_resolver.set_parent_namespace_resolver(parent_namespace_resolver_);
                  ParseNode *select_node = parse_res.result_tree_->children_[0];
                  if (OB_FAIL(select_resolver.resolve_child_stmt(*select_node))) {
                  } else {
                    stmt_ = select_resolver.get_child_stmt();
                    return OB_SUCCESS;
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  // ===== 拦截结束 =====

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
