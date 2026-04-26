<script setup>
import { computed, onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { 
  Search, 
  Plus, 
  Refresh, 
  Edit, 
  Delete, 
  User, 
  Key, 
  Lock,
  Unlock,
  Setting,
  Upload,
  Download,
  Warning
} from '@element-plus/icons-vue'
import api from '../api/client'

const tab = ref('users')

const usersLoading = ref(false)
const users = ref([])
const userQuery = ref('')
const roleFilter = ref('all')

const rolesOfMe = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) ? u.roles : []
  } catch {
    return []
  }
})
const isSystemAdmin = computed(() => rolesOfMe.value.includes('admin'))
const isDean = computed(() => rolesOfMe.value.includes('dean') && !rolesOfMe.value.includes('admin'))

const roleFilterOptions = computed(() => {
  const base = [
    { label: '全部角色', value: 'all' },
    { label: '教师', value: 'teacher' },
    { label: '学生', value: 'student' },
  ]
  if (isSystemAdmin.value) base.splice(1, 0, { label: '教务管理员', value: 'dean' })
  if (isSystemAdmin.value) base.splice(1, 0, { label: '管理员', value: 'admin' })
  return base
})

const rbacLoading = ref(false)
const roles = ref([])
const permissions = ref([])

const roleOptions = computed(() => (roles.value || []).map((r) => ({ label: r.name, value: r.name })))
const allowedRoleOptions = computed(() => {
  if (isSystemAdmin.value) return roleOptions.value
  return roleOptions.value.filter((r) => r.value === 'teacher' || r.value === 'student')
})
const permissionOptions = computed(() => (permissions.value || []).map((p) => ({ label: p.code, value: p.code })))

async function loadUsers() {
  usersLoading.value = true
  try {
    const role = roleFilter.value && roleFilter.value !== 'all' ? roleFilter.value : undefined
    const resp = await api.get('/api/admin/users', { params: { q: userQuery.value || undefined, role } })
    users.value = resp.data.items || []
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载用户失败')
  } finally {
    usersLoading.value = false
  }
}

async function loadRbac() {
  rbacLoading.value = true
  try {
    const resp = await api.get('/api/admin/rbac')
    roles.value = resp.data.roles || []
    permissions.value = resp.data.permissions || []
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载RBAC失败')
  } finally {
    rbacLoading.value = false
  }
}

const createOpen = ref(false)
const createForm = ref({ username: '', password: '', roles: ['student'], is_active: true, class_name: '' })

const bulkOpen = ref(false)
const uploadRef = ref(null)
const importResults = ref(null)
const importing = ref(false)

function openBulk() {
  bulkOpen.value = true
  importResults.value = null
}

function downloadTemplate() {
  const headers = 'username,password,roles,class_name'
  const example = 'student01,123456,student,计科2101\nteacher01,123456,teacher,'
  const content = headers + '\n' + example
  const blob = new Blob([content], { type: 'text/csv;charset=utf-8;' })
  const link = document.createElement('a')
  link.href = URL.createObjectURL(blob)
  link.setAttribute('download', 'account_import_template.csv')
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
}

async function handleImport() {
  if (!uploadRef.value) return
  const files = uploadRef.value.uploadFiles
  if (files.length === 0) {
    ElMessage.warning('请选择要上传的文件')
    return
  }

  const file = files[0].raw
  const formData = new FormData()
  formData.append('file', file)

  importing.value = true
  try {
    const resp = await api.post('/api/admin/users/bulk-import', formData, {
      headers: { 'Content-Type': 'multipart/form-data' }
    })
    importResults.value = resp.data
    if (resp.data.success > 0) {
      ElMessage.success(`成功导入 ${resp.data.success} 个账号`)
      await loadUsers()
    }
    if (resp.data.failed > 0) {
      ElMessage.warning(`${resp.data.failed} 个账号导入失败，请查看错误详情`)
    }
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '批量导入失败')
  } finally {
    importing.value = false
  }
}

function openCreate() {
  createForm.value = { username: '', password: '', roles: ['student'], is_active: true, class_name: '' }
  createOpen.value = true
}

async function submitCreate() {
  const username = (createForm.value.username || '').trim()
  const password = (createForm.value.password || '').trim()
  if (!username || !password) {
    ElMessage.warning('请输入用户名和密码')
    return
  }
  try {
    await api.post('/api/admin/users', {
      username,
      password,
      roles: createForm.value.roles || [],
      is_active: !!createForm.value.is_active,
      class_name: createForm.value.class_name || undefined,
    })
    ElMessage.success('账号创建成功')
    createOpen.value = false
    await loadUsers()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '创建失败')
  }
}

const editOpen = ref(false)
const editForm = ref({ id: null, username: '', roles: [], is_active: true, password: '', class_name: '' })

function openEdit(row) {
  editForm.value = {
    id: row.id,
    username: row.username,
    roles: Array.isArray(row.roles) ? [...row.roles] : [],
    is_active: !!row.is_active,
    password: '',
    class_name: row.class_name || '',
  }
  editOpen.value = true
}

async function submitEdit() {
  const id = editForm.value.id
  if (!id) return
  const payload = {
    roles: editForm.value.roles || [],
    is_active: !!editForm.value.is_active,
    class_name: editForm.value.class_name || '',
  }
  const pwd = (editForm.value.password || '').trim()
  if (pwd) payload.password = pwd
  try {
    await api.patch(`/api/admin/users/${id}`, payload)
    ElMessage.success('账号修改成功')
    editOpen.value = false
    await loadUsers()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '保存失败')
  }
}

async function removeUser(row) {
  try {
    await ElMessageBox.confirm(`确认删除账号「${row.username}」吗？此操作不可撤销。`, '删除确认', { 
      type: 'warning',
      confirmButtonText: '确定删除',
      cancelButtonText: '取消',
      confirmButtonClass: 'el-button--danger'
    })
  } catch {
    return
  }
  try {
    await api.delete(`/api/admin/users/${row.id}`)
    ElMessage.success('账号已删除')
    await loadUsers()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '删除失败')
  }
}

const roleEditOpen = ref(false)
const roleEditForm = ref({ id: null, name: '', permission_codes: [] })
const newPermCode = ref('')

function openRoleEdit(row) {
  roleEditForm.value = {
    id: row.id,
    name: row.name,
    permission_codes: Array.isArray(row.permissions) ? [...row.permissions] : [],
  }
  roleEditOpen.value = true
}

async function submitRolePerms() {
  const id = roleEditForm.value.id
  if (!id) return
  try {
    await api.put(`/api/admin/roles/${id}/permissions`, { permission_codes: roleEditForm.value.permission_codes || [] })
    ElMessage.success('权限更新成功')
    roleEditOpen.value = false
    await loadRbac()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '保存失败')
  }
}

async function createPermission() {
  const code = (newPermCode.value || '').trim()
  if (!code) return
  try {
    await api.post('/api/admin/permissions', { code })
    newPermCode.value = ''
    await loadRbac()
    ElMessage.success('新增权限码成功')
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '新增权限码失败')
  }
}

onMounted(async () => {
  if (isSystemAdmin.value) {
    await loadRbac()
  } else {
    roles.value = [{ id: -1, name: 'teacher', permissions: [] }, { id: -2, name: 'student', permissions: [] }]
    permissions.value = []
  }
  await loadUsers()
})
</script>

<template>
  <div class="admin-users-container">
    <el-card class="main-card" shadow="never">
      <template #header>
        <div class="card-header">
          <div class="title-section">
            <el-icon><Setting /></el-icon>
            <span>账号与权限中心</span>
          </div>
          <el-button 
            :icon="Refresh" 
            @click="() => Promise.all([isSystemAdmin ? loadRbac() : Promise.resolve(), loadUsers()])"
            size="small"
          >
            同步数据
          </el-button>
        </div>
      </template>

      <el-tabs v-model="tab" class="admin-tabs">
        <!-- User Management -->
        <el-tab-pane label="账号列表" name="users">
          <div class="table-toolbar">
            <div class="left-tools">
              <el-select v-model="roleFilter" placeholder="角色筛选" style="width: 140px" @change="loadUsers">
                <el-option v-for="opt in roleFilterOptions" :key="opt.value" :label="opt.label" :value="opt.value" />
              </el-select>
              <el-input 
                v-model="userQuery" 
                placeholder="搜索用户名" 
                style="width: 240px" 
                clearable 
                :prefix-icon="Search"
                @keyup.enter="loadUsers" 
              />
              <el-button type="primary" :icon="Search" @click="loadUsers">查询</el-button>
            </div>
            <div class="right-tools">
              <el-button type="warning" plain :icon="Upload" @click="openBulk">批量导入</el-button>
              <el-button type="success" :icon="Plus" @click="openCreate">新增账号</el-button>
            </div>
          </div>

          <el-table :data="users" v-loading="usersLoading" border stripe style="width: 100%" class="data-table">
            <el-table-column prop="username" label="用户名" min-width="140">
              <template #default="{ row }">
                <div class="user-cell">
                  <el-icon><User /></el-icon>
                  <span>{{ row.username }}</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="所属角色" min-width="180">
              <template #default="{ row }">
                <div class="role-tags">
                  <el-tag v-for="r in row.roles || []" :key="r" size="small" effect="plain">{{ r }}</el-tag>
                  <span v-if="!(row.roles || []).length">-</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="班级信息" width="160">
              <template #default="{ row }">
                <el-tag v-if="row.class_name" type="info" size="small" round>{{ row.class_name }}</el-tag>
                <span v-else>-</span>
              </template>
            </el-table-column>
            <el-table-column label="账号状态" width="110" align="center">
              <template #default="{ row }">
                <el-tag :type="row.is_active ? 'success' : 'danger'" size="small">
                  {{ row.is_active ? '启用中' : '已禁用' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="created_at" label="创建日期" width="180">
              <template #default="{ row }">
                {{ row.created_at ? new Date(row.created_at).toLocaleDateString() : '-' }}
              </template>
            </el-table-column>
            <el-table-column label="管理操作" width="160" fixed="right" align="center">
              <template #default="{ row }">
                <el-button-group>
                  <el-button size="small" :icon="Edit" @click="openEdit(row)">编辑</el-button>
                  <el-button size="small" type="danger" :icon="Delete" @click="removeUser(row)"></el-button>
                </el-button-group>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- RBAC Management -->
        <el-tab-pane v-if="isSystemAdmin" label="角色与权限定义" name="rbac">
          <div class="rbac-toolbar">
            <el-input 
              v-model="newPermCode" 
              placeholder="请输入新的权限码（例如: resource.delete）" 
              style="width: 360px" 
              clearable 
              :prefix-icon="Key"
            />
            <el-button type="primary" :icon="Plus" @click="createPermission">新增权限码</el-button>
          </div>

          <el-table :data="roles" v-loading="rbacLoading" border stripe style="width: 100%" class="data-table">
            <el-table-column prop="name" label="角色名称" width="180">
              <template #default="{ row }">
                <div class="role-cell">
                  <el-icon><Lock /></el-icon>
                  <span>{{ row.name }}</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="关联权限码列表" min-width="400">
              <template #default="{ row }">
                <div class="perm-tags">
                  <el-tag v-for="p in row.permissions || []" :key="p" size="small" type="warning" effect="plain">
                    {{ p }}
                  </el-tag>
                  <span v-if="!(row.permissions || []).length" class="empty-text">未配置权限</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="管理" width="140" align="center">
              <template #default="{ row }">
                <el-button size="small" :icon="Edit" @click="openRoleEdit(row)">配置权限</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
      </el-tabs>
    </el-card>

    <!-- Create User Dialog -->
    <el-dialog v-model="createOpen" title="新增账号" width="480px" destroy-on-close>
      <el-form :model="createForm" label-position="top">
        <el-form-item label="用户名" required>
          <el-input v-model="createForm.username" placeholder="建议使用教工号/学号" :prefix-icon="User" />
        </el-form-item>
        <el-form-item label="初始密码" required>
          <el-input v-model="createForm.password" placeholder="请输入初始密码" show-password :prefix-icon="Lock" />
        </el-form-item>
        <el-form-item label="分配角色" required>
          <el-select v-model="createForm.roles" multiple filterable style="width: 100%" placeholder="可选择多个角色">
            <el-option v-for="opt in allowedRoleOptions" :key="opt.value" :label="opt.label" :value="opt.value" />
          </el-select>
        </el-form-item>
        <el-row :gutter="20">
          <el-col :span="12">
            <el-form-item label="账号状态">
              <el-switch v-model="createForm.is_active" active-text="启用" inactive-text="禁用" />
            </el-form-item>
          </el-col>
          <el-col :span="12" v-if="createForm.roles.includes('student')">
            <el-form-item label="所属班级">
              <el-input v-model="createForm.class_name" placeholder="如: 计科2101" />
            </el-form-item>
          </el-col>
        </el-row>
      </el-form>
      <template #footer>
        <el-button @click="createOpen = false">取消</el-button>
        <el-button type="primary" @click="submitCreate">确认创建</el-button>
      </template>
    </el-dialog>

    <!-- Edit User Dialog -->
    <el-dialog v-model="editOpen" title="编辑账号信息" width="480px">
      <el-form :model="editForm" label-position="top">
        <el-form-item label="用户名">
          <el-input v-model="editForm.username" disabled :prefix-icon="User" />
        </el-form-item>
        <el-form-item label="修改角色">
          <el-select v-model="editForm.roles" multiple filterable style="width: 100%">
            <el-option v-for="opt in allowedRoleOptions" :key="opt.value" :label="opt.label" :value="opt.value" />
          </el-select>
        </el-form-item>
        <el-row :gutter="20">
          <el-col :span="12">
            <el-form-item label="账号状态">
              <el-switch v-model="editForm.is_active" active-text="启用" inactive-text="禁用" />
            </el-form-item>
          </el-col>
          <el-col :span="12" v-if="editForm.roles.includes('student')">
            <el-form-item label="班级信息">
              <el-input v-model="editForm.class_name" placeholder="如: 计科2101" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-form-item label="重置密码">
          <el-input v-model="editForm.password" placeholder="不修改请留空" show-password :prefix-icon="Lock" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="editOpen = false">取消</el-button>
        <el-button type="primary" @click="submitEdit">保存更改</el-button>
      </template>
    </el-dialog>

    <!-- Role Permissions Dialog -->
    <el-dialog v-model="roleEditOpen" title="配置角色权限" width="600px">
      <el-form :model="roleEditForm" label-position="top">
        <el-form-item label="正在编辑的角色">
          <el-tag effect="dark" type="warning">{{ roleEditForm.name }}</el-tag>
        </el-form-item>
        <el-form-item label="勾选/搜索权限码">
          <el-select 
            v-model="roleEditForm.permission_codes" 
            multiple 
            filterable 
            style="width: 100%" 
            placeholder="请选择权限码"
          >
            <el-option v-for="opt in permissionOptions" :key="opt.value" :label="opt.label" :value="opt.value" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="roleEditOpen = false">取消</el-button>
        <el-button type="primary" @click="submitRolePerms">应用更改</el-button>
      </template>
    </el-dialog>

    <!-- Bulk Import Dialog -->
    <el-dialog v-model="bulkOpen" title="批量导入账号" width="560px" destroy-on-close>
      <div class="bulk-import-container">
        <el-alert
          title="导入说明"
          type="info"
          description="请上传 CSV 或 Excel (.xlsx) 文件。文件必须包含：username, password, roles (角色名，多个用逗号分隔) 字段。学生角色可选填 class_name。"
          show-icon
          :closable="false"
          style="margin-bottom: 20px"
        >
          <template #default>
            <div style="margin-top: 10px">
              <el-button type="primary" link :icon="Download" @click="downloadTemplate">下载导入模板 (.csv)</el-button>
            </div>
          </template>
        </el-alert>
        
        <el-upload
          ref="uploadRef"
          class="bulk-upload"
          drag
          action="#"
          :auto-upload="false"
          :limit="1"
          accept=".csv,.xlsx,.xls"
        >
          <el-icon class="el-icon--upload"><Upload /></el-icon>
          <div class="el-upload__text">
            将文件拖到此处，或<em>点击上传</em>
          </div>
          <template #tip>
            <div class="el-upload__tip">
              支持 CSV, XLSX 格式，单次限 1 个文件
            </div>
          </template>
        </el-upload>

        <div v-if="importResults" class="import-results">
          <el-divider>导入结果</el-divider>
          <div class="result-stats">
            <el-tag type="success">成功: {{ importResults.success }}</el-tag>
            <el-tag type="danger" style="margin-left: 10px">失败: {{ importResults.failed }}</el-tag>
          </div>
          <div v-if="importResults.errors.length > 0" class="error-list">
            <div class="error-item" v-for="(err, idx) in importResults.errors" :key="idx">
              <el-icon color="#f56c6c"><Warning /></el-icon>
              <span>{{ err }}</span>
            </div>
          </div>
        </div>
      </div>
      <template #footer>
        <el-button @click="bulkOpen = false">关闭</el-button>
        <el-button type="primary" :loading="importing" @click="handleImport">开始导入</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.admin-users-container {
  padding: 0;
}

.main-card {
  border-radius: 12px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.title-section {
  display: flex;
  align-items: center;
  gap: 10px;
  font-weight: 600;
  font-size: 18px;
  color: #303133;
}

.admin-tabs {
  margin-top: 10px;
}

.table-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  gap: 16px;
  flex-wrap: wrap;
}

.left-tools {
  display: flex;
  gap: 12px;
}

.rbac-toolbar {
  display: flex;
  gap: 12px;
  margin-bottom: 20px;
}

.data-table {
  border-radius: 8px;
  overflow: hidden;
}

.user-cell, .role-cell {
  display: flex;
  align-items: center;
  gap: 8px;
  color: #606266;
}

.role-tags, .perm-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.empty-text {
  font-size: 13px;
  color: #c0c4cc;
  font-style: italic;
}

:deep(.el-tabs__item.is-active) {
  font-weight: 600;
}

:deep(.el-table__header) {
  background-color: #f5f7fa;
}
.right-tools {
  display: flex;
  gap: 12px;
}

.bulk-import-container {
  padding: 0 10px;
}

.import-results {
  margin-top: 20px;
}

.result-stats {
  text-align: center;
  margin-bottom: 15px;
}

.error-list {
  max-height: 200px;
  overflow-y: auto;
  background: #fff;
  border: 1px solid #ebeef5;
  border-radius: 4px;
  padding: 10px;
}

.error-item {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  font-size: 13px;
  color: #606266;
  margin-bottom: 8px;
}

.error-item span {
  flex: 1;
  word-break: break-all;
}
</style>
