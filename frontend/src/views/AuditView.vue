<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import api from '../api/client'

const loading = ref(false)
const items = ref([])
const deleteRequests = ref([])
const selectedRows = ref([])
const deleting = ref(false)
const approvingSelected = ref(false)
const deleteRequestLoading = ref(false)
const statusCounts = ref({ pending: 0, rejected: 0, approved: 0 })
const pagination = ref({ page: 1, pageSize: 10, total: 0 })
const deleteRequestPagination = ref({ total: 0 })

const state = reactive({
  status: 'pending',
  mode: 'resource',
})

const dialog = reactive({
  open: false,
  target: null,
  nextStatus: '',
  comment: '',
  submitting: false,
})

const roles = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) ? u.roles : []
  } catch {
    return []
  }
})
const isSystemAdmin = computed(() => roles.value.includes('admin'))
const isDean = computed(() => roles.value.includes('dean'))
const canAudit = computed(() => isDean.value)
const canBatchDelete = computed(() => isDean.value || isSystemAdmin.value)
const canReviewDeleteRequests = computed(() => isDean.value || isSystemAdmin.value)
const hasPendingResourceReview = computed(() => statusCounts.value.pending > 0)
const hasPendingDeleteRequest = computed(() => deleteRequestPagination.value.total > 0)
const resourceReviewCount = computed(() => statusCounts.value.pending + statusCounts.value.rejected + statusCounts.value.approved)
const deleteRequestCount = computed(() => deleteRequestPagination.value.total)
const pendingCount = computed(() => statusCounts.value.pending)
const rejectedCount = computed(() => statusCounts.value.rejected)
const approvedCount = computed(() => statusCounts.value.approved)

const STATUS_META = {
  pending: { label: '待审核', type: 'warning' },
  rejected: { label: '已拒绝', type: 'danger' },
  approved: { label: '已通过', type: 'success' },
}

const statusTabs = computed(() => [
  { key: 'pending', count: pendingCount.value, ...STATUS_META.pending },
  { key: 'rejected', count: rejectedCount.value, ...STATUS_META.rejected },
  { key: 'approved', count: approvedCount.value, ...STATUS_META.approved },
])

function statusTagType(status) {
  return STATUS_META[status]?.type || 'info'
}

function statusText(status) {
  return STATUS_META[status]?.label || status || '-'
}

function switchStatus(status) {
  if (state.status === status) return
  state.status = status
  state.mode = 'resource'
  pagination.value.page = 1
  fetchList()
}

function switchMode(mode) {
  if (state.mode === mode) return
  state.mode = mode
  if (mode === 'delete_request') {
    fetchDeleteRequests()
  } else {
    fetchList()
  }
}

if (isSystemAdmin.value && state.status === 'pending') state.status = 'rejected'

async function fetchList() {
  loading.value = true
  try {
    const resp = await api.get('/api/resources', {
      params: { status: state.status, page: pagination.value.page, page_size: pagination.value.pageSize },
    })
    items.value = resp.data.items || []
    pagination.value.total = resp.data.total ?? resp.data.items?.length ?? 0
    statusCounts.value = {
      pending: resp.data.status_counts?.pending ?? statusCounts.value.pending,
      rejected: resp.data.status_counts?.rejected ?? statusCounts.value.rejected,
      approved: resp.data.status_counts?.approved ?? statusCounts.value.approved,
    }
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

async function fetchDeleteRequests() {
  deleteRequestLoading.value = true
  try {
    const resp = await api.get('/api/admin/delete-requests')
    deleteRequests.value = resp.data.items || []
    deleteRequestPagination.value.total = deleteRequests.value.length
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载删除申请失败')
  } finally {
    deleteRequestLoading.value = false
  }
}

function handlePageChange(page) {
  if (pagination.value.page === page) return
  pagination.value.page = page
  fetchList()
}

function handleSizeChange(size) {
  if (pagination.value.pageSize === size) return
  pagination.value.pageSize = size
  pagination.value.page = 1
  fetchList()
}

function onSelectionChange(rows) {
  const nextRows = Array.isArray(rows) ? rows : []
  selectedRows.value = nextRows === selectedRows.value ? selectedRows.value : nextRows
}

function openAudit(row, nextStatus) {
  dialog.open = true
  dialog.target = row
  dialog.nextStatus = nextStatus
  dialog.comment = ''
}

async function batchDelete() {
  if (!canBatchDelete.value) return
  const ids = (selectedRows.value || []).map((r) => r?.id).filter((x) => typeof x === 'number' && Number.isFinite(x))
  if (ids.length === 0) {
    ElMessage.warning('请先勾选要删除的资源')
    return
  }
  try {
    await ElMessageBox.confirm(
      `确认批量删除已选 ${ids.length} 个资源？将同步删除资源关联关系与知识图谱中的对应内容，且不可恢复。`,
      '删除确认',
      { type: 'warning', confirmButtonText: '删除', cancelButtonText: '取消', confirmButtonClass: 'el-button--danger' },
    )
  } catch {
    return
  }
  deleting.value = true
  try {
    const resp = await api.post('/api/resources/batch-delete', { ids })
    const d = resp.data?.deleted || {}
    ElMessage.success(`批量删除完成（资源:${d.resources || 0} 文件:${d.resource_files || 0} 关联:${(d.tags || 0) + (d.resource_teachers || 0)}）`)
    selectedRows.value = []
    await fetchList()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '批量删除失败')
  } finally {
    deleting.value = false
  }
}

async function batchApproveSelected() {
  if (!isDean.value) return
  const ids = (selectedRows.value || [])
    .filter((r) => r?.status === 'pending')
    .map((r) => r?.id)
    .filter((x) => typeof x === 'number' && Number.isFinite(x))

  if (ids.length === 0) {
    ElMessage.warning('请先勾选要通过的待审核资源')
    return
  }

  try {
    await ElMessageBox.confirm(
      `确认一键通过已选中的 ${ids.length} 个待审核资源吗？未勾选的资源不会被处理。`,
      '批量通过确认',
      { type: 'success', confirmButtonText: '确定通过', cancelButtonText: '取消' },
    )
  } catch {
    return
  }

  approvingSelected.value = true
  try {
    let count = 0
    for (const id of ids) {
      await api.patch(`/api/resources/${id}/audit`, { status: 'approved' })
      count += 1
    }
    ElMessage.success(`成功通过 ${count} 个资源`)
    selectedRows.value = []
    await fetchList()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '批量通过失败')
  } finally {
    approvingSelected.value = false
  }
}

async function submitAudit() {
  if (!dialog.target) return
  dialog.submitting = true
  try {
    await api.patch(`/api/resources/${dialog.target.id}/audit`, {
      status: dialog.nextStatus,
      comment: dialog.comment || undefined,
    })
    ElMessage.success('已提交')
    dialog.open = false
    dialog.target = null
    await fetchList()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '提交失败')
  } finally {
    dialog.submitting = false
  }
}

async function approveDeleteRequest(row) {
  try {
    await ElMessageBox.confirm(
      `确认通过删除申请并删除资源《${row.resource_title || row.title}》吗？此操作会同步删除资源、文件和图谱节点，且不可恢复。`,
      '通过删除申请',
      { type: 'warning', confirmButtonText: '确定删除', cancelButtonText: '取消' },
    )
    await api.post(`/api/admin/delete-requests/${row.notification_id}/approve`)
    ElMessage.success('已通过删除申请')
    await fetchDeleteRequests()
  } catch (e) {
    if (e !== 'cancel' && e !== 'close') ElMessage.error(e?.response?.data?.error?.message || '操作失败')
  }
}

async function rejectDeleteRequest(row) {
  try {
    await ElMessageBox.confirm(
      `确认拒绝删除申请《${row.resource_title || row.title}》吗？`,
      '拒绝删除申请',
      { type: 'warning', confirmButtonText: '确定拒绝', cancelButtonText: '取消' },
    )
    await api.post(`/api/admin/delete-requests/${row.notification_id}/reject`)
    ElMessage.success('已拒绝删除申请')
    await fetchDeleteRequests()
  } catch (e) {
    if (e !== 'cancel' && e !== 'close') ElMessage.error(e?.response?.data?.error?.message || '操作失败')
  }
}

onMounted(() => {
  fetchList()
  fetchDeleteRequests()
})
</script>

<template>
  <div class="page-view audit-page">
    <el-card class="main-card audit-shell audit-fixed" shadow="hover">
      <template #header>
        <div class="page-toolbar audit-toolbar">
          <div class="page-toolbar__title title-section">
            <el-icon :size="22" class="title-icon"><Warning /></el-icon>
            <div class="title-block">
              <span class="title">资源审核</span>
              <span class="title-hint">支持资源审核与删除申请审核，删除申请会用醒目标识区分</span>
            </div>
          </div>
          <div class="toolbar-actions audit-actions">
            <el-tag v-if="!canAudit && !isSystemAdmin" type="warning">无审核权限</el-tag>
            <div class="audit-actions__fixed">
              <el-button-group>
                <el-button :type="state.mode === 'resource' ? 'primary' : 'default'" @click="switchMode('resource')">
                  资源审核（{{ pendingCount }}）
                  <span v-if="hasPendingResourceReview" class="mode-dot" aria-hidden="true"></span>
                </el-button>
                <el-button :type="state.mode === 'delete_request' ? 'danger' : 'default'" @click="switchMode('delete_request')">
                  删除申请（{{ deleteRequestCount }}）
                  <span v-if="hasPendingDeleteRequest" class="mode-dot mode-dot--danger" aria-hidden="true"></span>
                </el-button>
              </el-button-group>
              <div v-if="state.mode === 'resource'" class="status-switcher">
                <el-button-group>
                  <el-button
                    v-for="tab in statusTabs"
                    :key="tab.key"
                    :type="state.status === tab.key ? tab.type : 'default'"
                    :disabled="tab.key === 'pending' ? !canAudit : tab.key === 'approved' ? !canBatchDelete : false"
                    @click="switchStatus(tab.key)"
                  >
                    {{ tab.label }}（{{ tab.count }}）
                  </el-button>
                </el-button-group>
              </div>
              <div class="audit-actions__primary">
                <el-button type="primary" :loading="loading" @click="fetchList">刷新</el-button>
              </div>
            </div>
            <el-button v-if="isDean && state.status === 'pending' && state.mode === 'resource'" type="success" :loading="approvingSelected" :disabled="selectedRows.length === 0" @click="batchApproveSelected">
              批量通过
            </el-button>
            <el-button v-if="state.mode === 'resource' && canBatchDelete" type="danger" :disabled="(selectedRows || []).length === 0" :loading="deleting" @click="batchDelete">批量删除</el-button>
          </div>
        </div>
      </template>

      <el-table v-if="state.mode === 'resource'" :data="items" v-loading="loading" style="width: 100%" @selection-change="onSelectionChange">
        <el-table-column v-if="canBatchDelete" type="selection" width="44" />
        <el-table-column prop="title" label="标题" min-width="240">
          <template #default="{ row }">
            <div class="resource-title-cell">
              <div class="resource-title-main">{{ row.title }}</div>
              <div class="resource-title-sub">{{ row.course || '通用课程' }}</div>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="知识点" min-width="180">
          <template #default="{ row }">
            <div class="tag-wrap">
              <template v-if="(row.knowledge_points || []).length > 0">
                <el-tag v-for="kp in row.knowledge_points" :key="kp.id" size="small" effect="plain" type="info">{{ kp.name }}</el-tag>
              </template>
              <span v-else class="empty-text">{{ row.knowledge_point || '-' }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="教师" min-width="180">
          <template #default="{ row }">
            <div class="tag-wrap">
              <span v-if="(row.teachers || []).length === 0" class="empty-text">-</span>
              <el-tag v-for="t in row.teachers || []" :key="t.id" size="small" effect="plain">{{ t.name }}</el-tag>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="标签" min-width="180">
          <template #default="{ row }">
            <div class="tag-wrap">
              <el-tag v-for="t in row.tags || []" :key="t" size="small" effect="plain" type="warning">{{ t }}</el-tag>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="status" label="状态" width="110">
          <template #default="{ row }">
            <el-tag :type="statusTagType(row.status)" effect="dark">{{ statusText(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="240" fixed="right">
          <template #default="{ row }">
            <div class="audit-row-actions">
              <el-button v-if="canAudit && row.status === 'pending'" size="small" type="success" @click="openAudit(row, 'approved')">通过</el-button>
              <el-button v-if="canAudit && row.status === 'pending'" size="small" type="danger" @click="openAudit(row, 'rejected')">拒绝</el-button>
              <el-button v-if="canAudit && row.status === 'rejected'" size="small" @click="openAudit(row, 'approved')">改为通过</el-button>
            </div>
          </template>
        </el-table-column>
      </el-table>

      <div v-else class="delete-request-panel">
        <el-table :data="deleteRequests" v-loading="deleteRequestLoading" style="width: 100%">
          <el-table-column label="申请标识" width="160">
            <template #default>
              <el-tag type="danger" effect="dark">删除申请</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="resource_title" label="资源标题" min-width="220" />
          <el-table-column prop="resource_course" label="所属课程" min-width="180" />
          <el-table-column prop="content" label="申请说明" min-width="320">
            <template #default="{ row }">
              <div class="delete-request-text">{{ row.content }}</div>
            </template>
          </el-table-column>
          <el-table-column label="状态" width="140">
            <template #default>
              <el-tag type="warning" effect="dark">待审核</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="220" fixed="right">
            <template #default="{ row }">
              <div class="audit-row-actions">
                <el-button size="small" type="success" @click="approveDeleteRequest(row)">通过删除</el-button>
                <el-button size="small" type="danger" @click="rejectDeleteRequest(row)">拒绝</el-button>
              </div>
            </template>
          </el-table-column>
        </el-table>
      </div>

      <div class="table-pagination">
        <el-pagination
          v-model:current-page="pagination.page"
          v-model:page-size="pagination.pageSize"
          :total="pagination.total"
          :page-sizes="[10, 20, 50]"
          layout="共, sizes, prev, pager, next, jumper"
          background
          @current-change="handlePageChange"
          @size-change="handleSizeChange"
        />
      </div>
    </el-card>

    <el-dialog v-model="dialog.open" title="审核" width="520px">
      <el-form label-width="80px">
        <el-form-item label="资源">
          <div>{{ dialog.target?.title }}</div>
        </el-form-item>
        <el-form-item label="结果">
          <el-tag v-if="dialog.nextStatus === 'approved'" type="success">通过</el-tag>
          <el-tag v-else type="danger">拒绝</el-tag>
        </el-form-item>
        <el-form-item label="备注">
          <el-input v-model="dialog.comment" type="textarea" :rows="4" placeholder="可选" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialog.open = false">取消</el-button>
        <el-button type="primary" :loading="dialog.submitting" @click="submitAudit">提交</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style>
.audit-actions {
  flex-wrap: nowrap;
}

.audit-actions__fixed {
  display: flex;
  align-items: center;
  gap: 12px;
  flex: 0 0 auto;
  min-width: max-content;
}

.audit-actions__primary {
  width: 88px;
  display: flex;
  justify-content: flex-start;
}

.audit-actions__primary .el-button {
  width: 88px;
}

.status-switcher {
  flex: 0 0 auto;
  min-width: max-content;
}

.mode-dot {
  display: inline-block;
  width: 8px;
  height: 8px;
  margin-left: 6px;
  border-radius: 999px;
  background: #f56c6c;
  box-shadow: 0 0 0 3px rgba(245, 108, 108, 0.16);
  vertical-align: middle;
}

.mode-dot--danger {
  background: #f56c6c;
}

.delete-request-panel {
  margin-top: 4px;
}

.delete-request-text {
  color: #606266;
  line-height: 1.6;
}

.delete-request-panel :deep(.el-table__body tr:hover > td) {
  background-color: rgba(245, 108, 108, 0.06) !important;
}
</style>
