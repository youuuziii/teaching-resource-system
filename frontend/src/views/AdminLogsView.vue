<script setup>
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { ArrowDown, Search, Refresh, View } from '@element-plus/icons-vue'
import api from '../api/client'

const loading = ref(false)
const items = ref([])
const retentionDays = ref(7)
const pagination = ref({ page: 1, pageSize: 12, total: 0 })

// 过滤条件
const filters = ref({
  method: '',
  path: '',
  status_code: '',
  username: ''
})

const isAdmin = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) && u.roles.includes('admin')
  } catch {
    return false
  }
})

const retentionLabel = computed(() => `${retentionDays.value}天`)

function handleRetentionChange(val) {
  retentionDays.value = Number(val)
  pagination.value.page = 1
  fetchLogs()
}

async function fetchLogs() {
  loading.value = true
  try {
    const resp = await api.get('/api/admin/logs', {
      params: {
        page: pagination.value.page,
        page_size: pagination.value.pageSize,
        retention_days: retentionDays.value,
        ...filters.value
      },
    })
    items.value = resp.data.items || []
    pagination.value.total = resp.data.total ?? resp.data.items?.length ?? 0
    retentionDays.value = resp.data.retention_days ?? retentionDays.value
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

function handleSearch() {
  pagination.value.page = 1
  fetchLogs()
}

function resetFilters() {
  filters.value = {
    method: '',
    path: '',
    status_code: '',
    username: ''
  }
  handleSearch()
}

function handlePageChange(page) {
  pagination.value.page = page
  fetchLogs()
}

function handleSizeChange(size) {
  pagination.value.pageSize = size
  pagination.value.page = 1
  fetchLogs()
}

// Meta 详情
const metaDialogVisible = ref(false)
const currentMeta = ref({})

function showMeta(meta) {
  currentMeta.value = meta
  metaDialogVisible.value = true
}

onMounted(fetchLogs)
</script>

<template>
  <el-card class="main-card logs-fixed" shadow="hover">
    <template #header>
      <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap; justify-content: space-between">
        <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
          <div style="font-weight: 600; font-size: 18px">系统操作日志</div>
          <el-tag v-if="!isAdmin" type="warning">需要管理员权限</el-tag>
          <div class="retention-label-group">
            <span>数据保留期：</span>
            <el-dropdown trigger="click" @command="handleRetentionChange">
              <span class="retention-chip" :class="{ 'is-loading': loading }">
                {{ retentionLabel }}
                <el-icon class="el-icon--right"><ArrowDown /></el-icon>
              </span>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item :disabled="retentionDays === 3" command="3">3天</el-dropdown-item>
                  <el-dropdown-item :disabled="retentionDays === 7" command="7">7天</el-dropdown-item>
                  <el-dropdown-item :disabled="retentionDays === 30" command="30">30天</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </div>
        </div>
        <div style="display: flex; align-items: center; gap: 10px; flex-wrap: wrap">
          <el-button type="primary" :loading="loading" :icon="Refresh" @click="fetchLogs">刷新</el-button>
        </div>
      </div>
    </template>

    <!-- 过滤器栏 -->
    <div class="filter-bar">
      <el-row :gutter="20">
        <el-col :xs="24" :sm="12" :md="6" :lg="4">
          <el-select v-model="filters.method" placeholder="请求方法" clearable @change="handleSearch">
            <el-option label="GET" value="GET" />
            <el-option label="POST" value="POST" />
            <el-option label="PUT" value="PUT" />
            <el-option label="DELETE" value="DELETE" />
          </el-select>
        </el-col>
        <el-col :xs="24" :sm="12" :md="6" :lg="5">
          <el-input v-model="filters.username" placeholder="操作人" clearable @keyup.enter="handleSearch" :prefix-icon="Search" />
        </el-col>
        <el-col :xs="24" :sm="12" :md="6" :lg="6">
          <el-input v-model="filters.path" placeholder="请求路径" clearable @keyup.enter="handleSearch" :prefix-icon="Search" />
        </el-col>
        <el-col :xs="24" :sm="12" :md="6" :lg="4">
          <el-input v-model="filters.status_code" placeholder="状态码" clearable @keyup.enter="handleSearch" />
        </el-col>
        <el-col :xs="24" :sm="12" :md="6" :lg="5" class="filter-actions">
          <el-button type="primary" :icon="Search" @click="handleSearch">查询</el-button>
          <el-button :icon="Refresh" @click="resetFilters">重置</el-button>
        </el-col>
      </el-row>
    </div>

    <el-table :data="items" v-loading="loading" style="width: 100%" border stripe>
      <el-table-column prop="created_at" label="操作时间" width="180">
        <template #default="{ row }">
          {{ new Date(row.created_at).toLocaleString() }}
        </template>
      </el-table-column>
      <el-table-column label="操作人" min-width="150">
        <template #default="{ row }">
          <span>{{ row.username }}</span>
          <el-text size="small" type="info" style="margin-left: 8px">
            (ID: {{ row.user_id || 'System' }})
          </el-text>
        </template>
      </el-table-column>
      <el-table-column prop="method" label="方法" width="100">
        <template #default="{ row }">
          <el-tag :type="row.method === 'GET' ? 'info' : row.method === 'POST' ? 'success' : 'warning'">
            {{ row.method }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="status_code" label="状态" width="100">
        <template #default="{ row }">
          <el-tag :type="row.status_code >= 400 ? 'danger' : 'success'">
            {{ row.status_code }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="path" label="路径" min-width="200" show-overflow-tooltip />
      <el-table-column label="操作详情" width="120" fixed="right">
        <template #default="{ row }">
          <el-button size="small" type="primary" plain :icon="View" @click="showMeta(row.meta)">查看详情</el-button>
        </template>
      </el-table-column>
    </el-table>

    <div class="table-pagination">
      <el-pagination
        v-model:current-page="pagination.page"
        v-model:page-size="pagination.pageSize"
        :total="pagination.total"
        :page-sizes="[12, 20, 50, 100]"
        layout="total, sizes, prev, pager, next, jumper"
        background
        @current-change="handlePageChange"
        @size-change="handleSizeChange"
      />
    </div>

    <!-- Meta 详情对话框 -->
    <el-dialog v-model="metaDialogVisible" title="日志 Meta 详情" width="500px">
      <div class="meta-content">
        <pre>{{ JSON.stringify(currentMeta, null, 2) }}</pre>
      </div>
      <template #footer>
        <el-button @click="metaDialogVisible = false">关闭</el-button>
      </template>
    </el-dialog>
  </el-card>
</template>

<style scoped>
.filter-bar {
  margin-bottom: 20px;
  padding: 15px;
  background-color: #f8f9fa;
  border-radius: 8px;
}

.filter-actions {
  display: flex;
  gap: 10px;
}

.retention-label-group {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: #606266;
  font-size: 14px;
}

.retention-chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 5px 12px;
  border-radius: 999px;
  background: linear-gradient(135deg, #ecf5ff 0%, #d9ecff 100%);
  color: #2b6cb0;
  font-weight: 700;
  border: 1px solid rgba(64, 158, 255, 0.18);
  cursor: pointer;
  transition: all 0.2s ease;
  user-select: none;
  box-shadow: 0 4px 12px rgba(64, 158, 255, 0.12);
}

.retention-chip:hover {
  transform: translateY(-1px);
  box-shadow: 0 8px 18px rgba(64, 158, 255, 0.18);
}

.retention-chip.is-loading {
  opacity: 0.7;
  cursor: wait;
}

.table-pagination {
  margin-top: 20px;
  display: flex;
  justify-content: flex-end;
}

.meta-content {
  background-color: #f5f7fa;
  padding: 15px;
  border-radius: 4px;
  max-height: 400px;
  overflow-y: auto;
}

.meta-content pre {
  margin: 0;
  white-space: pre-wrap;
  word-wrap: break-word;
  font-family: monospace;
}
</style>
