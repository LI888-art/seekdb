/*
 * Copyright (c) 2025 OceanBase.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_ai_prompt.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprAIPrompt::ObExprAIPrompt(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_AI_PROMPT, N_AI_PROMPT, MORE_THAN_ZERO, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprAIPrompt::~ObExprAIPrompt()
{
}

int ObExprAIPrompt::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types_stack,
                                      int64_t param_num,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(param_num < 1)) {
    ret = OB_ERR_PARAM_SIZE;
  } else {
    // 强制所有参数为字符串或 JSON
    for (int64_t i = 0; i < param_num; ++i) {
      if (ob_is_string_type(types_stack[i].get_type())) {
        types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      } else if (types_stack[i].get_type() == ObJsonType) {
        // JSON 类型允许通过
      } else {
        types_stack[i].set_calc_type(ObVarcharType);
        types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
    type.set_type(ObJsonType);
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_length(OB_MAX_LONGTEXT_LENGTH);
  }
  return ret;
}

int ObExprAIPrompt::eval_ai_prompt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());

  if (OB_UNLIKELY(expr.arg_cnt_ < 1)) {
    ret = OB_ERR_PARAM_SIZE;
  } else {
    ObString template_str;
    ObJsonArray *args_array = nullptr;
    ObJsonObject *prompt_obj = nullptr;

    if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(temp_allocator, args_array))) {
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      ObDatum *datum = nullptr;
      if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {
      } else if (i == 0) {
        // 解析第一个参数（模板）
        if (OB_ISNULL(datum) || datum->is_null()) {
          template_str = ObString("");
        } else if (expr.args_[0]->datum_meta_.type_ == ObJsonType) {
          ObIJsonBase *j_base = nullptr;
          bool is_null = false;
          if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 0, j_base, is_null))) {
          } else if (OB_ISNULL(j_base)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (j_base->json_type() == ObJsonNodeType::J_STRING) {
            template_str.assign_ptr(j_base->get_data(), static_cast<int32_t>(j_base->get_data_length()));
          } else {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
          }
        } else {
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, temp_allocator, *datum, expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), template_str))) {
          }
        }
      } else {
        // 解析后续参数（替换值）
        if (OB_ISNULL(datum) || datum->is_null()) {
          ObJsonNull *null_node = nullptr;
          if (OB_ISNULL(null_node = OB_NEWx(ObJsonNull, &temp_allocator))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (OB_FAIL(args_array->append(null_node))) {
          }
        } else if (expr.args_[i]->datum_meta_.type_ == ObJsonType) {
          ObIJsonBase *j_base = nullptr;
          bool is_null = false;
          if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, i, j_base, is_null))) {
          } else if (OB_ISNULL(j_base)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(args_array->append(static_cast<ObJsonNode*>(j_base)))) {
          }
        } else {
          ObString str_val;
          ObJsonString *str_node = nullptr;
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, temp_allocator, *datum, expr.args_[i]->datum_meta_, expr.args_[i]->obj_meta_.has_lob_header(), str_val))) {
          } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(temp_allocator, str_val, str_node))) {
          } else if (OB_FAIL(args_array->append(str_node))) {
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObAIFuncPromptObjectUtils::construct_prompt_object(temp_allocator, template_str, args_array, prompt_obj))) {
      } else if (OB_ISNULL(prompt_obj)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObString raw_str;
        if (OB_FAIL(prompt_obj->get_raw_binary(raw_str, &temp_allocator))) {
        } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_str))) {
        }
      }
    }
  }
  return ret;
}

int ObExprAIPrompt::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprAIPrompt::eval_ai_prompt;
  return ret;
}

} // namespace sql
} // namespace oceanbase
