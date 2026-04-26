<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import api from '../api/client'

const loading = ref(false)
const items = ref([])
const selectedRows = ref([])
const deleting = ref(false)
const approvingAll = ref(false)

const state = reactive({
  status: 'pending',
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

if (isSystemAdmin.value && state.status === 'pending') state.status = 'rejected'

async function fetchList() {
  loading.value = true
  try {
    const resp = await api.get('/api/resources', { params: { status: state.status } })
    items.value = resp.data.items || []
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

function onSelectionChange(rows) {
  selectedRows.value = Array.isArray(rows) ? rows : []
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

async function batchApproveAll() {
  if (!isDean.value) return
  try {
    await ElMessageBox.confirm(
      '确认要一键通过所有待审核的资源吗？',
      '一键通过确认',
      { type: 'success', confirmButtonText: '确定通过', cancelButtonText: '取消' }
    )
  } catch {
    return
  }
  approvingAll.value = true
  try {
    const resp = await api.post('/api/resources/batch-approve-all')
    ElMessage.success(`成功一键通过 ${resp.data.count || 0} 个资源`)
    await fetchList()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '操作失败')
  } finally {
    approvingAll.value = false
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

onMounted(fetchList)
</script>

<template>
  <el-card>
    <template #header>
      <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
        <div style="font-weight: 600">资源审核</div>
        <el-tag v-if="!canAudit && !isSystemAdmin" type="warning">无审核权限</el-tag>
        <el-select v-model="state.status" style="width: 140px" @change="fetchList">
          <el-option v-if="canAudit" label="待审核" value="pending" />
          <el-option label="已拒绝" value="rejected" />
          <el-option v-if="canBatchDelete" label="已通过" value="approved" />
        </el-select>
        <el-button type="primary" :loading="loading" @click="fetchList">刷新</el-button>
        <el-button
          v-if="isDean && state.status === 'pending'"
          type="success"
          :loading="approvingAll"
          @click="batchApproveAll"
        >
          一键通过
        </el-button>
        <el-button
          v-if="canBatchDelete"
          type="danger"
          :disabled="(selectedRows || []).length === 0"
          :loading="deleting"
          @click="batchDelete"
        >
          批量删除
        </el-button>
      </div>
    </template>

    <el-table :data="items" v-loading="loading" style="width: 100%" @selection-change="onSelectionChange">
      <el-table-column v-if="canBatchDelete" type="selection" width="44" />
      <el-table-column prop="title" label="标题" min-width="220" />
      <el-table-column prop="course" label="课程" min-width="120" />
      <el-table-column label="知识点" min-width="160">
        <template #default="{ row }">
          <template v-if="(row.knowledge_points || []).length > 0">
            <el-tag v-for="kp in row.knowledge_points" :key="kp.id" style="margin-right: 6px" size="small">
              {{ kp.name }}
            </el-tag>
          </template>
          <span v-else>{{ row.knowledge_point || '-' }}</span>
        </template>
      </el-table-column>
      <el-table-column label="教师" min-width="160">
        <template #default="{ row }">
          <span v-if="(row.teachers || []).length === 0">-</span>
          <el-tag v-for="t in row.teachers || []" :key="t.id" style="margin-right: 6px" size="small">
            {{ t.name }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="标签" min-width="160">
        <template #default="{ row }">
          <el-tag v-for="t in row.tags || []" :key="t" style="margin-right: 6px" size="small">{{ t }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="status" label="状态" width="110" />
      <el-table-column label="操作" width="220">
        <template #default="{ row }">
          <el-button v-if="canAudit && row.status === 'pending'" size="small" type="success" @click="openAudit(row, 'approved')">
            通过
          </el-button>
          <el-button v-if="canAudit && row.status === 'pending'" size="small" type="danger" @click="openAudit(row, 'rejected')">
            拒绝
          </el-button>
          <el-button v-if="canAudit && row.status === 'rejected'" size="small" @click="openAudit(row, 'approved')">改为通过</el-button>
        </template>
      </el-table-column>
    </el-table>
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
</template>
